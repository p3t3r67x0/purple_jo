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

# Add the project root to the Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.postgres import (  # noqa: E402
    Domain, ARecord, AAAARecord, NSRecord, MXRecord,
    SoaRecord, CNAMERecord
)


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 800  # Increased for better throughput
DEFAULT_CONCURRENCY = 500  # Increased for better parallelism
DEFAULT_TIMEOUT = 1.0  # Reduced for faster failure detection

DNS_RECORD_TYPES: Tuple[Tuple[str, str], ...] = (
    ("A", "a_record"),
    ("AAAA", "aaaa_record"),
    ("NS", "ns_record"),
    ("MX", "mx_record"),
    ("SOA", "soa_record"),
    ("CNAME", "cname_record"),
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

    def record(self, success: bool) -> None:
        self.processed += 1
        if success:
            self.succeeded += 1
        else:
            self.failed += 1

    def summary(self) -> str:
        return (
            f"processed={self.processed} succeeded={self.succeeded} "
            f"failed={self.failed}"
        )


class PostgresAsync:
    def __init__(self, postgres_dsn: str) -> None:
        # Convert sync postgresql:// URL to async postgresql+asyncpg://
        if postgres_dsn.startswith("postgresql://"):
            postgres_dsn = postgres_dsn.replace("postgresql://",
                                                "postgresql+asyncpg://")
        elif not postgres_dsn.startswith("postgresql+asyncpg://"):
            # If no scheme, assume we need asyncpg
            if "://" not in postgres_dsn:
                postgres_dsn = f"postgresql+asyncpg://{postgres_dsn}"

        self._engine = create_async_engine(
            postgres_dsn,
            echo=False,
            pool_size=20,  # Increased for better concurrency
            max_overflow=50,  # Higher overflow for burst traffic
            pool_timeout=30,
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

    async def close(self) -> None:
        await self.postgres.close()

    def _should_skip_domain(self, domain: str) -> bool:
        """Skip domains that are likely to fail or waste time."""
        # Skip obviously invalid domains
        if not domain or len(domain) < 3 or len(domain) > 253:
            return True
        
        # Skip domains with invalid characters
        if any(c in domain for c in ['<', '>', '"', '`', ' ', '\t']):
            return True
            
        # Skip localhost and IP addresses
        if domain in ('localhost', '127.0.0.1', '::1'):
            return True
            
        # Skip domains that are likely to be invalid
        invalid_patterns = [
            'xn--', 'www.www.', '-.', '.-', '..',
            'internal', 'local', 'test'
        ]
        if any(pattern in domain for pattern in invalid_patterns):
            return True
            
        return False

    async def process_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain or self._should_skip_domain(domain):
            return False

        records = await self._resolve_all(domain)
        now = utcnow()

        if records and any(records.values()):
            await self._store_records(domain, records, now)
            self._log_records(domain, records)
            self.stats.record(True)
            return True

        await self._mark_failed(domain, now)
        self.stats.record(False)
        log.debug("DNS lookup failed for %s", domain)
        return False

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
        timeout = self.settings.dns_timeout

        def _lookup() -> List[object]:
            result: List[object] = []
            # Reuse resolver instance for better performance
            if not hasattr(self, '_resolver'):
                self._resolver = resolver.Resolver()
                self._resolver.timeout = timeout
                self._resolver.lifetime = timeout
                # Use Google DNS for better reliability
                self._resolver.nameservers = ['8.8.8.8', '8.8.4.4']
            
            res = self._resolver
            try:
                items = res.resolve(domain, record_type)
            except (Timeout, LabelTooLong, NoNameservers, EmptyLabel, NoAnswer, NXDOMAIN):
                return result
            except DNSException:
                return result

            for item in items:
                if record_type not in {"MX", "NS", "SOA", "CNAME"}:
                    result.append(
                        getattr(item, "address", item.to_text()).lower())
                    continue

                if record_type == "NS":
                    result.append(item.target.to_unicode().strip(".").lower())
                elif record_type == "SOA":
                    text = item.to_text().replace("\\", "").lower()
                    if result:
                        result[0] = text
                    else:
                        result.append(text)
                elif record_type == "CNAME":
                    result.append(
                        {"target": item.target.to_unicode().strip(".").lower()})
                else:  # MX
                    result.append(
                        {
                            "preference": item.preference,
                            "exchange": item.exchange.to_unicode().lower().strip("."),
                        }
                    )

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
        async with await self.postgres.get_session() as session:
            try:
                # Find or create domain
                stmt = select(Domain).where(Domain.name == domain)
                result = await session.exec(stmt)
                domain_obj = result.first()

                if not domain_obj:
                    domain_obj = Domain(
                        name=domain,
                        created_at=timestamp,
                        updated_at=timestamp
                    )
                    session.add(domain_obj)
                    await session.flush()  # Get the ID
                else:
                    domain_obj.updated_at = timestamp
                    session.add(domain_obj)

                # Store DNS records
                await self._store_dns_records(session, domain_obj.id, records)
                await session.commit()

            except Exception as e:
                await session.rollback()
                log.error("Failed to store records for domain %s: %s",
                          domain, e)
                raise

    async def _store_dns_records(
        self,
        session,
        domain_id: int,
        records: Dict[str, List[object]]
    ) -> None:
        """Store DNS records in PostgreSQL tables using bulk operations"""
        # Prepare bulk record lists for better performance
        a_records = []
        aaaa_records = []
        ns_records = []
        mx_records = []
        soa_records = []
        cname_records = []
        
        for record_type, record_list in records.items():
            if not record_list:
                continue

            if record_type == "a":
                a_records.extend([
                    ARecord(domain_id=domain_id, ip_address=ip)
                    for ip in record_list
                ])
            elif record_type == "aaaa":
                aaaa_records.extend([
                    AAAARecord(domain_id=domain_id, ip_address=ip)
                    for ip in record_list
                ])
            elif record_type == "ns":
                ns_records.extend([
                    NSRecord(domain_id=domain_id, nameserver=ns)
                    for ns in record_list
                ])
            elif record_type == "mx":
                for mx in record_list:
                    if isinstance(mx, dict):
                        mx_records.append(MXRecord(
                            domain_id=domain_id,
                            exchange=mx.get('exchange'),
                            preference=mx.get('preference', 0)
                        ))
                    else:
                        mx_records.append(MXRecord(
                            domain_id=domain_id, exchange=str(mx)
                        ))
            elif record_type == "soa":
                for soa in record_list:
                    if isinstance(soa, dict):
                        soa_records.append(SoaRecord(
                            domain_id=domain_id,
                            mname=soa.get('mname'),
                            rname=soa.get('rname'),
                            serial=soa.get('serial'),
                            refresh=soa.get('refresh'),
                            retry=soa.get('retry'),
                            expire=soa.get('expire'),
                            minimum=soa.get('minimum')
                        ))
                    else:
                        soa_records.append(SoaRecord(
                            domain_id=domain_id, mname=str(soa)
                        ))
            elif record_type == "cname":
                cname_records.extend([
                    CNAMERecord(domain_id=domain_id, target=cname)
                    for cname in record_list
                ])
        
        # Bulk add all records at once
        if a_records:
            session.add_all(a_records)
        if aaaa_records:
            session.add_all(aaaa_records)
        if ns_records:
            session.add_all(ns_records)
        if mx_records:
            session.add_all(mx_records)
        if soa_records:
            session.add_all(soa_records)
        if cname_records:
            session.add_all(cname_records)

    async def _mark_failed(self, domain: str, timestamp: datetime) -> None:
        async with await self.postgres.get_session() as session:
            try:
                # Find or create domain
                stmt = select(Domain).where(Domain.name == domain)
                result = await session.exec(stmt)
                domain_obj = result.first()

                if not domain_obj:
                    domain_obj = Domain(
                        name=domain,
                        created_at=timestamp,
                        updated_at=timestamp
                    )
                    session.add(domain_obj)
                else:
                    domain_obj.updated_at = timestamp
                    session.add(domain_obj)

                await session.commit()
            except Exception as e:
                await session.rollback()
                log.error("Failed to mark domain as failed %s: %s", domain, e)
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

            task = asyncio.create_task(self._handle_job(job))
            self._pending.add(task)
            task.add_done_callback(self._pending.discard)

        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

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
    """Stream domains in batches to avoid memory overload."""
    offset = 0
    
    while True:
        async with await postgres.get_session() as session:
            # Get domains that don't have any A records processed yet
            stmt = select(Domain.name).outerjoin(ARecord).where(
                ARecord.domain_id.is_(None)
            ).offset(offset).limit(batch_size)

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
            stmt = select(Domain.name).outerjoin(ARecord).where(
                ARecord.domain_id.is_(None)
            ).offset(settings.skip).limit(settings.limit)

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

    base_settings = WorkerSettings(
        postgres_dsn=postgres_dsn,
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
    postgres = PostgresAsync(postgres_dsn)

    async def count_pending_domains():
        async with await postgres.get_session() as session:
            # Count domains that don't have any A records processed yet
            # (A records are usually the first to be processed)
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
    main()
