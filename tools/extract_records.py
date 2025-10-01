#!/usr/bin/env python3
"""DNS Record Extraction Coordinator.

This is the main coordination script that orchestrates DNS record extraction
using separate publisher and worker services. It can run in three modes:

1. Publisher mode: Queue domains to RabbitMQ for processing
2. Worker mode: Process DNS resolution jobs from RabbitMQ
3. Direct mode: Run DNS extraction without RabbitMQ (legacy)

For distributed processing, use dns_publisher_service.py and
dns_worker_service.py instead of this coordinator script.
"""

from __future__ import annotations

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

from importlib import import_module

try:
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

bootstrap.setup()

import asyncio
import contextlib
import logging
import math
import multiprocessing
import os
import sys
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from dns import resolver
from dns.exception import DNSException, Timeout
from dns.name import EmptyLabel, LabelTooLong
from dns.resolver import NoAnswer, NoNameservers, NXDOMAIN
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from shared.models.postgres import (  # noqa: E402
    Domain, ARecord, AAAARecord, NSRecord, MXRecord,
    SoaRecord, CNAMERecord, TXTRecord
)
from async_sqlmodel_helpers import normalise_async_dsn, resolve_async_dsn


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 800  # Increased for better throughput
DEFAULT_CONCURRENCY = 500  # Increased for better parallelism
DEFAULT_TIMEOUT = 1.0  # Reduced for faster failure detection

DNS_RECORD_TYPES = (
    ("A", "a_record"),
    ("AAAA", "aaaa_record"),
    ("NS", "ns_record"),
    ("MX", "mx_record"),
    ("SOA", "soa_record"),
    ("CNAME", "cname_record"),
    ("TXT", "txt_record"),
)


log = logging.getLogger("extract_records")


def utcnow() -> datetime:
    """Return current UTC datetime without timezone info."""
    return datetime.now(UTC).replace(tzinfo=None)


def configure_logging(level: int) -> None:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        root_logger.addHandler(handler)
    root_logger.setLevel(level)
    for handler in root_logger.handlers:
        handler.setLevel(level)
    log.setLevel(level)


@dataclass
class WorkerSettings:
    postgres_dsn: str
    rabbitmq_url: Optional[str]
    queue_name: str
    prefetch: int
    concurrency: int
    dns_timeout: float
    log_level: int
    verbose_records: bool = False


@dataclass
class DirectWorkerSettings(WorkerSettings):
    skip: int = 0
    limit: int = 0


@dataclass
class DNSJob:
    domain: str
    message: aio_pika.IncomingMessage
    stop: bool = False


@dataclass
class DNSStats:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    total_records: int = 0
    start_time: Optional[datetime] = None

    def __post_init__(self):
        if self.start_time is None:
            self.start_time = utcnow()

    def record(self, success: bool, record_count: int = 0) -> None:
        self.processed += 1
        if success:
            self.succeeded += 1
            self.total_records += record_count
        else:
            self.failed += 1

    def record_skip(self) -> None:
        self.skipped += 1

    def rate(self) -> float:
        """Calculate processing rate in domains per second."""
        if not self.start_time:
            return 0.0
        elapsed = (utcnow() - self.start_time).total_seconds()
        return self.processed / elapsed if elapsed > 0 else 0.0

    def summary(self) -> str:
        rate = self.rate()
        return (
            f"processed={self.processed} succeeded={self.succeeded} "
            f"failed={self.failed} skipped={self.skipped} "
            f"records={self.total_records} rate={rate:.1f}/sec"
        )


class PostgresAsync:
    def __init__(self, postgres_dsn: str) -> None:
        normalised = normalise_async_dsn(postgres_dsn)

        self._engine = create_async_engine(
            normalised,
            echo=False,
            pool_size=50,  # Significantly increased for high concurrency
            max_overflow=100,  # Higher overflow for burst traffic
            pool_timeout=60,  # Increased timeout for busy periods
            pool_recycle=3600,
            pool_pre_ping=True,
            # Performance optimizations
            connect_args={
                "server_settings": {
                    "jit": "off",  # Disable JIT for faster small queries
                    "application_name": "dns_extractor"
                }
            }
        )
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    async def get_session(self) -> AsyncSession:
        """Get a database session."""
        return self._session_factory()

    async def close(self) -> None:
        """Close the database connection."""
        if self._engine:
            await self._engine.dispose()


class DNSRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
        self.stats = DNSStats()
        self._resolver_pool = []
        self._initialize_resolver_pool()

    def _initialize_resolver_pool(self) -> None:
        """Initialize a pool of DNS resolvers for better performance."""
        pool_size = min(self.settings.concurrency, 50)  # Limit pool size

        for _ in range(pool_size):
            resolver_instance = resolver.Resolver()
            resolver_instance.timeout = self.settings.dns_timeout
            resolver_instance.lifetime = self.settings.dns_timeout * 2

            # Configure nameservers
            nameservers_env = os.environ.get("DNS_NAMESERVERS")
            if nameservers_env:
                resolver_instance.nameservers = [
                    ns.strip() for ns in nameservers_env.split(",")
                    if ns.strip()
                ]
            else:
                # Use multiple DNS servers for better performance
                resolver_instance.nameservers = [
                    '8.8.8.8', '8.8.4.4',  # Google DNS
                    '1.1.1.1', '1.0.0.1',  # Cloudflare DNS
                ]

            self._resolver_pool.append(resolver_instance)

    def _get_resolver(self) -> resolver.Resolver:
        """Get a resolver from the pool (round-robin)."""
        if not self._resolver_pool:
            self._initialize_resolver_pool()

        # Simple round-robin selection
        import threading
        if not hasattr(self, '_resolver_index'):
            self._resolver_index = 0
            self._resolver_lock = threading.Lock()

        with self._resolver_lock:
            resolver_instance = self._resolver_pool[self._resolver_index]
            self._resolver_index = (
                self._resolver_index + 1) % len(self._resolver_pool)

        return resolver_instance

    async def close(self) -> None:
        await self.postgres.close()

    def _should_skip_domain(self, domain: str) -> bool:
        """Skip domains that are likely to fail or waste time."""
        # Skip obviously invalid domains
        if not domain or len(domain) < 3 or len(domain) > 253:
            return True

        # Skip domains with invalid characters
        if any(c in domain for c in ['<', '>', '"', '`', ' ', '\t', '\n', '\r']):
            return True

        # Skip localhost and IP addresses
        if domain in ('localhost', '127.0.0.1', '::1'):
            return True

        # Skip domains that are likely to be invalid or problematic
        invalid_patterns = [
            'xn--', 'www.www.', '-.', '.-', '..', '--',
            'internal', 'local', 'test', 'example',
            'invalid', 'localhost', '192.168.',
        ]
        if any(pattern in domain for pattern in invalid_patterns):
            return True

        # Skip domains without valid TLD structure
        if '.' not in domain or domain.startswith('.') or domain.endswith('.'):
            return True

        # Skip domains with consecutive dots or invalid format
        if '..' in domain or domain.count('.') < 1:
            return True

        # Check for basic domain name validation
        import re
        domain_pattern = re.compile(
            r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?'
            r'(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$'
        )
        if not domain_pattern.match(domain):
            return True

        return False

    async def process_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain or self._should_skip_domain(domain):
            self.stats.record_skip()
            return False

        records = await self._resolve_all(domain)
        now = utcnow()

        # Use a single session for both success and failure cases
        try:
            if records and any(records.values()):
                record_count = sum(len(values) for values in records.values())
                await self._store_records(domain, records, now)
                self._log_records(domain, records)
                self.stats.record(True, record_count)
                return True
            else:
                await self._mark_failed(domain, now)
                self.stats.record(False)
                log.debug("DNS lookup failed for %s", domain)
                return False
        except Exception as e:
            log.error("Database error processing domain %s: %s", domain, e)
            self.stats.record(False)
            return False

    async def process_batch(self, domains: List[str]) -> Dict[str, bool]:
        """Process a batch of domains with controlled database concurrency."""
        # Limit database connections to prevent pool exhaustion
        db_semaphore = asyncio.Semaphore(
            min(20, self.settings.concurrency // 2))
        dns_semaphore = asyncio.Semaphore(self.settings.concurrency)

        async def process_single(domain: str) -> Tuple[str, bool]:
            # DNS resolution can be highly concurrent
            async with dns_semaphore:
                domain = domain.strip().lower()
                if not domain or self._should_skip_domain(domain):
                    self.stats.record_skip()
                    return domain, False

                records = await self._resolve_all(domain)
                now = utcnow()

                # Limit database operations to prevent connection pool exhaustion
                async with db_semaphore:
                    try:
                        if records and any(records.values()):
                            record_count = sum(len(values)
                                               for values in records.values())
                            await self._store_records(domain, records, now)
                            self._log_records(domain, records)
                            self.stats.record(True, record_count)
                            return domain, True
                        else:
                            await self._mark_failed(domain, now)
                            self.stats.record(False)
                            return domain, False
                    except Exception as e:
                        log.error(
                            "Database error processing %s: %s", domain, e)
                        self.stats.record(False)
                        return domain, False

        # Process all domains in the batch concurrently
        tasks = [process_single(domain)
                 for domain in domains if domain.strip()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        batch_results = {}
        for result in results:
            if isinstance(result, Exception):
                log.error("Batch processing error: %s", result)
                continue
            domain, success = result
            batch_results[domain] = success

        return batch_results

    async def _resolve_all(self, domain: str) -> Dict[str, List[object]]:
        # Use asyncio.gather for better performance
        tasks = [
            self._resolve_record(domain, record_type)
            for record_type, _ in DNS_RECORD_TYPES
        ]

        try:
            # Resolve all DNS record types concurrently
            results_list = await asyncio.gather(*tasks, return_exceptions=True)

            results: Dict[str, List[object]] = {}
            for i, (_, field_name) in enumerate(DNS_RECORD_TYPES):
                result = results_list[i]
                if isinstance(result, Exception):
                    log.debug("%s lookup crashed for %s: %s",
                              field_name, domain, result)
                    results[field_name] = []
                else:
                    results[field_name] = result or []

            return results
        except Exception as e:
            log.error("Failed to resolve DNS records for %s: %s", domain, e)
            return {field_name: [] for _, field_name in DNS_RECORD_TYPES}

    async def _resolve_record(self, domain: str,
                              record_type: str) -> List[object]:
        def _lookup() -> List[object]:
            result: List[object] = []
            # Get resolver from pool for better performance and concurrency
            res = self._get_resolver()

            try:
                items = res.resolve(domain, record_type)
            except (Timeout, LabelTooLong, NoNameservers, EmptyLabel,
                    NoAnswer, NXDOMAIN):
                return result
            except DNSException:
                return result

            for item in items:
                if record_type not in {"MX", "NS", "SOA", "CNAME"}:
                    result.append(
                        getattr(item, "address", item.to_text()).lower())
                    continue

                if record_type == "NS":
                    result.append(
                        item.target.to_unicode().strip(".").lower()
                    )
                elif record_type == "SOA":
                    text = item.to_text().replace("\\", "").lower()
                    if result:
                        result[0] = text
                    else:
                        result.append(text)
                elif record_type == "CNAME":
                    result.append({
                        "target": item.target.to_unicode().strip(".").lower()
                    })
                else:  # MX
                    result.append({
                        "preference": item.preference,
                        "exchange": item.exchange.to_unicode().lower().strip("."),
                    })

            return result

        values = await asyncio.to_thread(_lookup)
        if values:
            log.debug("resolved %s for %s: %s", record_type, domain, values)
        return values

    async def _store_records(
        self,
        domain: str,
        records: Dict[str, List[object]],
        timestamp: datetime,
    ) -> None:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with await self.postgres.get_session() as session:
                    try:
                        # Use more efficient get-or-create pattern with SQLAlchemy
                        stmt = select(Domain).where(Domain.name == domain)
                        result = await session.exec(stmt)
                        domain_obj = result.first()

                        if not domain_obj:
                            # Create new domain using SQLAlchemy ORM
                            domain_obj = Domain(
                                name=domain,
                                created_at=timestamp,
                                updated_at=timestamp
                            )
                            session.add(domain_obj)
                            await session.flush()  # Get the ID immediately
                            domain_id = domain_obj.id
                        else:
                            # Update existing domain using SQLAlchemy ORM
                            domain_obj.updated_at = timestamp
                            session.add(domain_obj)  # Mark for update
                            domain_id = domain_obj.id

                        # Store DNS records using bulk SQLAlchemy operations
                        await self._store_dns_records_bulk(session, domain_id, records)
                        await session.commit()
                        return  # Success, exit retry loop

                    except Exception as e:
                        await session.rollback()
                        raise e

            except Exception as e:
                if "QueuePool" in str(e) and attempt < max_retries - 1:
                    # Wait briefly before retrying connection pool errors
                    await asyncio.sleep(0.1 * (attempt + 1))
                    log.warning("Connection pool error, retrying %d/%d for %s: %s",
                                attempt + 1, max_retries, domain, e)
                    continue
                else:
                    log.error("Failed to store records for domain %s: %s",
                              domain, e)
                    raise

    async def _store_dns_records_bulk(
        self,
        session: AsyncSession,
        domain_id: int,
        records: Dict[str, List[object]]
    ) -> None:
        """Store DNS records using efficient SQLAlchemy bulk operations"""

        # Prepare bulk record lists using SQLModel ORM objects
        record_objects = []

        for record_type, record_list in records.items():
            if not record_list:
                continue

            # Use SQLAlchemy ORM objects for type safety and validation
            if record_type == "a_record":
                record_objects.extend([
                    ARecord(domain_id=domain_id, ip_address=ip)
                    for ip in record_list
                ])

            elif record_type == "aaaa_record":
                record_objects.extend([
                    AAAARecord(domain_id=domain_id, ip_address=ip)
                    for ip in record_list
                ])

            elif record_type == "ns_record":
                record_objects.extend([
                    NSRecord(domain_id=domain_id, value=ns)
                    for ns in record_list
                ])

            elif record_type == "mx_record":
                for mx in record_list:
                    if isinstance(mx, dict):
                        record_objects.append(MXRecord(
                            domain_id=domain_id,
                            exchange=mx.get('exchange'),
                            priority=mx.get('preference', 0)
                        ))
                    else:
                        record_objects.append(MXRecord(
                            domain_id=domain_id,
                            exchange=str(mx),
                            priority=0
                        ))

            elif record_type == "soa_record":
                record_objects.extend([
                    SoaRecord(domain_id=domain_id, value=str(soa))
                    for soa in record_list
                ])

            elif record_type == "cname_record":
                for cname in record_list:
                    if isinstance(cname, dict):
                        target = cname.get('target', str(cname))
                    else:
                        target = str(cname)
                    record_objects.append(CNAMERecord(
                        domain_id=domain_id,
                        target=target
                    ))

            elif record_type == "txt_record":
                record_objects.extend([
                    TXTRecord(domain_id=domain_id, content=txt)
                    for txt in record_list
                ])

        # Use SQLAlchemy's efficient bulk add operation
        if record_objects:
            session.add_all(record_objects)

    async def _mark_failed(self, domain: str, timestamp: datetime) -> None:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with await self.postgres.get_session() as session:
                    try:
                        # Use SQLAlchemy ORM for efficient get-or-create pattern
                        stmt = select(Domain).where(Domain.name == domain)
                        result = await session.exec(stmt)
                        domain_obj = result.first()

                        if not domain_obj:
                            # Create new domain using SQLAlchemy ORM
                            domain_obj = Domain(
                                name=domain,
                                created_at=timestamp,
                                updated_at=timestamp
                            )
                            session.add(domain_obj)
                        else:
                            # Update existing domain using SQLAlchemy ORM
                            domain_obj.updated_at = timestamp
                            session.add(domain_obj)  # Mark for update

                        await session.commit()
                        return  # Success, exit retry loop

                    except Exception as e:
                        await session.rollback()
                        raise e

            except Exception as e:
                if "QueuePool" in str(e) and attempt < max_retries - 1:
                    # Wait briefly before retrying connection pool errors
                    await asyncio.sleep(0.1 * (attempt + 1))
                    log.warning("Connection pool error, retrying %d/%d for %s",
                                attempt + 1, max_retries, domain)
                    continue
                else:
                    log.error(
                        "Failed to mark domain as failed %s: %s", domain, e)
                    raise

    def _log_records(self, domain: str, records: Dict[str, List[object]]) -> None:
        total = sum(len(values) for values in records.values())
        log.info("updated DNS records for %s (total=%d)", domain, total)
        if self.settings.verbose_records:
            for field, values in sorted(records.items()):
                log.info("%s records for %s: %s", field, domain, values)


class DNSConsumer:
    def __init__(self, settings: WorkerSettings, runtime: DNSRuntime) -> None:
        self.settings = settings
        self.runtime = runtime
        self._semaphore = asyncio.Semaphore(max(1, settings.concurrency))
        self._pending: Set[asyncio.Task[None]] = set()
        self._stopped = asyncio.Event()
        self._last_progress_report = utcnow()
        self._progress_interval = 30  # Report progress every 30 seconds

    async def consume(self) -> None:
        log.info(
            "Worker %s consuming queue '%s' with prefetch=%d",
            os.getpid(),
            self.settings.queue_name,
            self.settings.prefetch,
        )

        async for job in get_domains(self.settings):
            if self._stopped.is_set():
                with contextlib.suppress(Exception):
                    await job.message.reject(requeue=True)
                continue

            if job.stop:
                with contextlib.suppress(Exception):
                    await job.message.ack()
                self._stopped.set()
                log.debug("Worker %s received STOP", os.getpid())
                break

            # Report progress periodically
            self._maybe_report_progress()

            task = asyncio.create_task(self._handle_job(job))
            self._pending.add(task)
            task.add_done_callback(self._pending.discard)

        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

        # Final progress report
        self._report_progress(force=True)

    def _maybe_report_progress(self) -> None:
        """Report progress if enough time has passed."""
        now = utcnow()
        elapsed = (now - self._last_progress_report).total_seconds()

        if elapsed >= self._progress_interval:
            self._report_progress()
            self._last_progress_report = now

    def _report_progress(self, force: bool = False) -> None:
        """Report current processing statistics."""
        stats = self.runtime.stats
        if force or stats.processed > 0:
            log.info(
                "Worker %s progress: %s",
                os.getpid(),
                stats.summary()
            )

    async def _handle_job(self, job: DNSJob) -> None:
        try:
            async with self._semaphore:
                await self.runtime.process_domain(job.domain)
        except Exception as exc:  # pragma: no cover - defensive
            log.exception(
                "Unhandled error while resolving %s: %s", job.domain, exc)
        finally:
            with contextlib.suppress(Exception):
                await job.message.ack()


async def get_domains(settings: WorkerSettings) -> AsyncIterator[DNSJob]:
    if not settings.rabbitmq_url:
        raise ValueError("RabbitMQ URL is required to consume DNS jobs")

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max(1, settings.prefetch))
        queue = await channel.declare_queue(settings.queue_name, durable=True)

        async with queue.iterator() as iterator:
            async for message in iterator:
                if message.body == STOP_SENTINEL:
                    yield DNSJob(domain="", message=message, stop=True)
                    break

                domain = message.body.decode().strip()
                if not domain:
                    await message.ack()
                    continue

                yield DNSJob(domain=domain, message=message)
    finally:
        await connection.close()


async def enqueue_domains(
    rabbitmq_url: str,
    queue_name: str,
    domains: AsyncIterator[str],
    worker_count: int,
    purge_queue: bool,
) -> int:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    sent = 0
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)
        if purge_queue:
            await queue.purge()
        exchange = channel.default_exchange
        async for domain in domains:
            domain = domain.strip()
            if not domain:
                continue
            message = Message(domain.encode("utf-8"),
                              delivery_mode=DeliveryMode.PERSISTENT)
            await exchange.publish(message, routing_key=queue_name)
            sent += 1
            if sent and sent % 1000 == 0:
                log.info("Queued %d domains so far", sent)

        stop_message = Message(
            STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


async def iter_pending_domains(
    postgres: PostgresAsync,
    batch_size: int = 10_000,
) -> AsyncIterator[str]:
    """Stream domains in batches using efficient SQLAlchemy queries."""
    offset = 0

    while True:
        async with await postgres.get_session() as session:
            # Use SQLAlchemy LEFT JOIN to find unprocessed domains efficiently
            stmt = select(Domain.name).outerjoin(ARecord).where(
                ARecord.domain_id.is_(None)
            ).offset(offset).limit(batch_size).order_by(Domain.id)

            result = await session.exec(stmt)
            domains = result.all()

            if not domains:
                break

            for domain in domains:
                yield domain

            offset += len(domains)

            # If we got fewer than batch_size, we're done
            if len(domains) < batch_size:
                break


async def direct_worker(settings: DirectWorkerSettings) -> str:
    runtime = DNSRuntime(settings)
    try:
        # Get domains that don't have any A records processed yet
        async with await runtime.postgres.get_session() as session:
            # Use SQLAlchemy query with proper ordering for consistent results
            stmt = select(Domain.name).outerjoin(ARecord).where(
                ARecord.domain_id.is_(None)
            ).offset(settings.skip).limit(settings.limit).order_by(Domain.id)

            result = await session.exec(stmt)
            domains = result.all()

        pending: Set[asyncio.Task[None]] = set()
        semaphore = asyncio.Semaphore(max(1, settings.concurrency))

        for domain in domains:
            if not domain:
                continue

            async def _process(target: str) -> None:
                async with semaphore:
                    await runtime.process_domain(target)

            task = asyncio.create_task(_process(domain))
            pending.add(task)
            task.add_done_callback(pending.discard)

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return runtime.stats.summary()
    finally:
        await runtime.close()


async def service_worker(settings: WorkerSettings) -> str:
    runtime = DNSRuntime(settings)
    try:
        consumer = DNSConsumer(settings, runtime)
        await consumer.consume()
        return runtime.stats.summary()
    finally:
        await runtime.close()


def run_worker(settings: WorkerSettings) -> str:
    configure_logging(settings.log_level)
    log.info(
        "Worker process %s starting (mode=rabbit, concurrency=%d)",
        os.getpid(),
        settings.concurrency,
    )
    try:
        summary = asyncio.run(service_worker(settings))
        log.info("Worker %s finished: %s", os.getpid(), summary)
        return f"Worker {os.getpid()} done ({summary})"
    except AMQPConnectionError as exc:
        log.error("RabbitMQ connection failed: %s", exc)
        return f"[ERROR] Worker could not connect to RabbitMQ: {exc}"
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("Worker process %s crashed", os.getpid())
        return f"[ERROR] Worker crashed: {exc}"


def run_worker_direct(settings: DirectWorkerSettings) -> str:
    configure_logging(settings.log_level)
    log.info(
        "Worker process %s starting (mode=direct, slice=%d:%d, concurrency=%d)",
        os.getpid(),
        settings.skip,
        settings.skip + settings.limit,
        settings.concurrency,
    )
    try:
        summary = asyncio.run(direct_worker(settings))
        log.info("Worker %s finished: %s", os.getpid(), summary)
        return f"Worker {os.getpid()} done ({summary})"
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("Worker process %s crashed", os.getpid())
        return f"[ERROR] Worker crashed: {exc}"


@click.command()
@click.option("--worker", "-w", type=int, default=4, show_default=True, help="Worker processes")
@click.option("--postgres-dsn", "-p", type=str, required=True,
              help="PostgreSQL connection string")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY, show_default=True,
              help="Concurrent DNS lookups per worker")
@click.option("--dns-timeout", type=float, default=DEFAULT_TIMEOUT, show_default=True,
              help="DNS resolver timeout in seconds")
@click.option("--rabbitmq-url", "-r", type=str, default=None,
              help="RabbitMQ URL (enables distributed mode)")
@click.option("--queue-name", "-q", type=str, default="dns_records", show_default=True,
              help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
              help="RabbitMQ prefetch per worker")
@click.option("--purge-queue/--no-purge-queue", default=True,
              help="Purge the queue before enqueuing new domains")
@click.option("--service", is_flag=True,
              help="Run as a RabbitMQ-consuming DNS resolution service")
@click.option("--log-records", is_flag=True,
              help="Log extracted records at info level")
@click.option("--verbose", is_flag=True, help="Enable verbose debug logging")
def main(
    worker: int,
    postgres_dsn: str,
    concurrency: int,
    dns_timeout: float,
    rabbitmq_url: Optional[str],
    queue_name: str,
    prefetch: int,
    purge_queue: bool,
    service: bool,
    log_records: bool,
    verbose: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    worker = max(1, worker)
    concurrency = max(1, concurrency)
    prefetch = max(1, prefetch)

    resolved_dsn = resolve_async_dsn(postgres_dsn)

    base_settings = WorkerSettings(
        postgres_dsn=resolved_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        concurrency=concurrency,
        dns_timeout=dns_timeout,
        log_level=log_level,
        verbose_records=log_records,
    )

    if service:
        if not rabbitmq_url:
            raise click.BadParameter(
                "RabbitMQ URL is required when --service is set")

        worker_args = [WorkerSettings(**base_settings.__dict__)
                       for _ in range(worker)]
        if worker == 1:
            click.echo(run_worker(worker_args[0]))
        else:
            with multiprocessing.Pool(processes=worker) as pool:
                log.info("Spawned %d worker processes (service mode)", worker)
                for result in pool.imap_unordered(run_worker, worker_args):
                    click.echo(result)
        return

    # Create PostgreSQL connection for counting
    postgres = PostgresAsync(resolved_dsn)

    async def count_pending_domains():
        async with await postgres.get_session() as session:
            # Use efficient SQLAlchemy aggregate query with LEFT JOIN
            # Count domains that don't have any A records processed yet
            stmt = select(func.count(Domain.id)).outerjoin(ARecord).where(
                ARecord.domain_id.is_(None)
            )
            result = await session.exec(stmt)
            return result.first()

    total_docs = asyncio.run(count_pending_domains())
    click.echo(f"[INFO] total domains to resolve: {total_docs}")

    if total_docs == 0:
        click.echo("[INFO] Nothing to resolve")
        return

    if rabbitmq_url:
        domains = iter_pending_domains(postgres)
        log.info("Publishing DNS jobs to RabbitMQ at %s", rabbitmq_url)
        published = asyncio.run(
            enqueue_domains(
                rabbitmq_url=rabbitmq_url,
                queue_name=queue_name,
                domains=domains,
                worker_count=worker,
                purge_queue=purge_queue,
            )
        )
        log.info("Queued %d domains onto RabbitMQ queue '%s'",
                 published, queue_name)

        worker_args = [WorkerSettings(**base_settings.__dict__)
                       for _ in range(worker)]
        with multiprocessing.Pool(processes=worker) as pool:
            log.info("Spawned %d worker processes", worker)
            for result in pool.imap_unordered(run_worker, worker_args):
                click.echo(result)
    else:
        chunk_size = math.ceil(total_docs / worker)
        tasks = []
        start = 0
        while start < total_docs:
            limit = min(chunk_size, total_docs - start)
            settings = DirectWorkerSettings(
                **base_settings.__dict__,
                skip=start,
                limit=limit,
            )
            tasks.append(settings)
            start += limit

        with multiprocessing.Pool(processes=worker) as pool:
            log.info("Spawned %d direct worker processes", worker)
            for result in pool.imap_unordered(run_worker_direct, tasks):
                click.echo(result)


def run_separate_services():
    """
    For better performance and scalability, use the separate services:

    1. Start publisher service:
       python tools/dns_publisher_service.py --postgres-dsn "..." --rabbitmq-url "..."

    2. Start worker services (can run multiple instances):
       python tools/dns_worker_service.py --postgres-dsn "..." --rabbitmq-url "..." --worker-id "worker-1"
       python tools/dns_worker_service.py --postgres-dsn "..." --rabbitmq-url "..." --worker-id "worker-2"

    This approach provides:
    - Better horizontal scaling
    - Independent service management
    - Fault isolation
    - Resource optimization
    """
    pass


if __name__ == "__main__":
    CLITool(main).run()
