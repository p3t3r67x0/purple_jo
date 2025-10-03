#!/usr/bin/env python3
"""RabbitMQ-backed DNS record extraction microservice."""

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
import multiprocessing
import os
import re
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import AsyncIterator, Dict, List, Optional, Set

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from dns import resolver
from dns.exception import DNSException, Timeout
from dns.name import EmptyLabel, LabelTooLong
from dns.resolver import NoAnswer, NoNameservers, NXDOMAIN
from sqlalchemy import func
from sqlmodel import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from shared.models.postgres import (
    AAAARecord,
    ARecord,
    CNAMERecord,
    Domain,
    MXRecord,
    NSRecord,
    SoaRecord,
    TXTRecord,
)
from async_sqlmodel_helpers import normalise_async_dsn, resolve_async_dsn

STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 800
DEFAULT_CONCURRENCY = 500
DEFAULT_DNS_TIMEOUT = 1.0
DEFAULT_QUEUE_NAME = "dns_records"
DEFAULT_BATCH_SIZE = 10_000

log = logging.getLogger("extract_records")

DNS_RECORD_TYPES = (
    ("A", "a_record"),
    ("AAAA", "aaaa_record"),
    ("NS", "ns_record"),
    ("MX", "mx_record"),
    ("SOA", "soa_record"),
    ("CNAME", "cname_record"),
    ("TXT", "txt_record"),
)


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass
class WorkerSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    prefetch: int
    concurrency: int
    dns_timeout: float
    log_level: int
    verbose_records: bool


@dataclass
class PublisherSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    worker_count: int
    purge_queue: bool
    batch_size: int = DEFAULT_BATCH_SIZE


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

    def __post_init__(self) -> None:
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
        if not self.start_time:
            return 0.0
        elapsed = (utcnow() - self.start_time).total_seconds()
        return self.processed / elapsed if elapsed > 0 else 0.0

    def summary(self) -> str:
        return (
            f"processed={self.processed} succeeded={self.succeeded} "
            f"failed={self.failed} skipped={self.skipped} "
            f"records={self.total_records} rate={self.rate():.1f}/sec"
        )


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


class PostgresAsync:
    def __init__(self, postgres_dsn: str) -> None:
        normalised = normalise_async_dsn(postgres_dsn)
        self._engine = create_async_engine(
            normalised,
            echo=False,
            pool_size=50,
            max_overflow=100,
            pool_timeout=60,
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={
                "server_settings": {
                    "jit": "off",
                    "application_name": "dns_extractor",
                }
            },
        )
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    async def get_session(self) -> AsyncSession:
        return self._session_factory()

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()


class DNSRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
        self.stats = DNSStats()
        self._resolver_pool: List[resolver.Resolver] = []
        self._initialize_resolver_pool()

    def _initialize_resolver_pool(self) -> None:
        pool_size = min(self.settings.concurrency, 50)
        for _ in range(pool_size):
            resolver_instance = resolver.Resolver()
            resolver_instance.timeout = self.settings.dns_timeout
            resolver_instance.lifetime = self.settings.dns_timeout * 2

            nameservers_env = os.environ.get("DNS_NAMESERVERS")
            if nameservers_env:
                resolver_instance.nameservers = [ns.strip() for ns in nameservers_env.split(",") if ns.strip()]
            else:
                resolver_instance.nameservers = ['8.8.8.8', '8.8.4.4', '1.1.1.1', '1.0.0.1']

            self._resolver_pool.append(resolver_instance)

    def _get_resolver(self) -> resolver.Resolver:
        if not self._resolver_pool:
            self._initialize_resolver_pool()
        import threading
        if not hasattr(self, "_resolver_index"):
            self._resolver_index = 0
            self._resolver_lock = threading.Lock()
        with self._resolver_lock:
            resolver_instance = self._resolver_pool[self._resolver_index]
            self._resolver_index = (self._resolver_index + 1) % len(self._resolver_pool)
        return resolver_instance

    async def close(self) -> None:
        await self.postgres.close()

    def _should_skip_domain(self, domain: str) -> bool:
        if not domain or len(domain) < 3 or len(domain) > 253:
            return True
        if any(c in domain for c in ['<', '>', '"', '`', ' ', '\t', '\n', '\r']):
            return True
        if domain in ('localhost', '127.0.0.1', '::1'):
            return True
        invalid_patterns = [
            'xn--', 'www.www.', '-.', '.-', '..', '--',
            'internal', 'local', 'test', 'example',
            'invalid', 'localhost', '192.168.',
        ]
        if any(pattern in domain for pattern in invalid_patterns):
            return True
        if '.' not in domain or domain.startswith('.') or domain.endswith('.'):
            return True
        if '..' in domain or domain.count('.') < 1:
            return True
        domain_pattern = re.compile(
            r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?'
            r'(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$'
        )
        return not domain_pattern.match(domain)

    async def process_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain or self._should_skip_domain(domain):
            self.stats.record_skip()
            return False

        records = await self._resolve_all(domain)
        now = utcnow()
        try:
            if records and any(records.values()):
                record_count = sum(len(values) for values in records.values())
                await self._store_records(domain, records, now)
                self._log_records(domain, records)
                self.stats.record(True, record_count)
                return True
            await self._mark_failed(domain, now)
            self.stats.record(False)
            log.debug("DNS lookup failed for %s", domain)
            return False
        except Exception as exc:  # pragma: no cover - defensive
            log.error("Database error processing domain %s: %s", domain, exc)
            self.stats.record(False)
            return False

    async def _resolve_all(self, domain: str) -> Dict[str, List[object]]:
        tasks = [self._resolve_record(domain, record_type) for record_type, _ in DNS_RECORD_TYPES]
        results = await asyncio.gather(*tasks)
        return {field_name: result for ( _, field_name), result in zip(DNS_RECORD_TYPES, results)}

    async def _resolve_record(self, domain: str, record_type: str) -> List[object]:
        def _lookup() -> List[object]:
            result: List[object] = []
            res = self._get_resolver()
            try:
                items = res.resolve(domain, record_type)
            except (Timeout, LabelTooLong, NoNameservers, EmptyLabel, NoAnswer, NXDOMAIN):
                return result
            except DNSException:
                return result

            for item in items:
                if record_type not in {"MX", "NS", "SOA", "CNAME"}:
                    result.append(getattr(item, "address", item.to_text()).lower())
                    continue
                if record_type == "NS":
                    result.append(item.target.to_unicode().strip(".").lower())
                elif record_type == "SOA":
                    result.append(item.to_text().replace("\\", "").lower())
                elif record_type == "CNAME":
                    result.append({"target": item.target.to_unicode().strip(".").lower()})
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
        async with await self.postgres.get_session() as session:
            stmt = select(Domain).where(Domain.name == domain)
            result = await session.exec(stmt)
            domain_obj = result.first()

            if not domain_obj:
                domain_obj = Domain(name=domain, created_at=timestamp, updated_at=timestamp)
                session.add(domain_obj)
                await session.flush()
            else:
                domain_obj.updated_at = timestamp
                session.add(domain_obj)

            await self._store_dns_records_bulk(session, domain_obj.id, records)
            await session.commit()

    async def _mark_failed(self, domain: str, timestamp: datetime) -> None:
        async with await self.postgres.get_session() as session:
            stmt = select(Domain).where(Domain.name == domain)
            result = await session.exec(stmt)
            domain_obj = result.first()

            if not domain_obj:
                domain_obj = Domain(name=domain, created_at=timestamp, updated_at=timestamp)
            else:
                domain_obj.updated_at = timestamp

            session.add(domain_obj)
            await session.commit()

    def _log_records(self, domain: str, records: Dict[str, List[object]]) -> None:
        total = sum(len(values) for values in records.values())
        log.info("updated DNS records for %s (total=%d)", domain, total)
        if self.settings.verbose_records:
            for field, values in sorted(records.items()):
                log.info("%s records for %s: %s", field, domain, values)

    async def _store_dns_records_bulk(
        self,
        session: AsyncSession,
        domain_id: int,
        records: Dict[str, List[object]],
    ) -> None:
        record_objects = []
        for record_type, record_list in records.items():
            if not record_list:
                continue
            if record_type == "a_record":
                record_objects.extend(ARecord(domain_id=domain_id, ip_address=str(ip)) for ip in record_list)
            elif record_type == "aaaa_record":
                record_objects.extend(AAAARecord(domain_id=domain_id, ip_address=str(ip)) for ip in record_list)
            elif record_type == "ns_record":
                record_objects.extend(NSRecord(domain_id=domain_id, value=str(ns)) for ns in record_list)
            elif record_type == "mx_record":
                for mx in record_list:
                    if isinstance(mx, dict):
                        record_objects.append(MXRecord(
                            domain_id=domain_id,
                            exchange=str(mx.get("exchange")),
                            priority=int(mx.get("preference", 0)),
                        ))
                    else:
                        record_objects.append(MXRecord(domain_id=domain_id, exchange=str(mx), priority=0))
            elif record_type == "soa_record":
                record_objects.extend(SoaRecord(domain_id=domain_id, value=str(soa)) for soa in record_list)
            elif record_type == "cname_record":
                for cname in record_list:
                    target = cname.get("target") if isinstance(cname, dict) else str(cname)
                    record_objects.append(CNAMERecord(domain_id=domain_id, target=str(target)))
            elif record_type == "txt_record":
                record_objects.extend(TXTRecord(domain_id=domain_id, content=str(txt)) for txt in record_list)
        if record_objects:
            session.add_all(record_objects)


class DNSConsumer:
    def __init__(self, settings: WorkerSettings, runtime: DNSRuntime) -> None:
        self.settings = settings
        self.runtime = runtime
        self._semaphore = asyncio.Semaphore(max(1, settings.concurrency))
        self._pending: Set[asyncio.Task[None]] = set()
        self._stopped = asyncio.Event()
        self._last_progress_report = utcnow()
        self._progress_interval = 30

    async def consume(self) -> None:
        connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
        try:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=max(1, self.settings.prefetch))
            queue = await channel.declare_queue(self.settings.queue_name, durable=True)

            async with queue.iterator() as iterator:
                async for message in iterator:
                    if message.body == STOP_SENTINEL:
                        await message.ack()
                        self._stopped.set()
                        log.debug("Received STOP sentinel, worker exiting")
                        break

                    domain = message.body.decode().strip()
                    if not domain:
                        await message.ack()
                        continue

                    task = asyncio.create_task(self._handle_domain(domain, message))
                    self._pending.add(task)
                    task.add_done_callback(self._pending.discard)

                    if (utcnow() - self._last_progress_report).total_seconds() >= self._progress_interval:
                        log.info("Progress: %s", self.runtime.stats.summary())
                        self._last_progress_report = utcnow()

            if self._pending:
                await asyncio.gather(*self._pending, return_exceptions=True)
        finally:
            await connection.close()

    async def _handle_domain(self, domain: str, message: aio_pika.IncomingMessage) -> None:
        try:
            async with self._semaphore:
                await self.runtime.process_domain(domain)
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("Unhandled error while resolving %s: %s", domain, exc)
        finally:
            with contextlib.suppress(Exception):
                await message.ack()


async def iter_pending_domains(postgres_dsn: str, batch_size: int = DEFAULT_BATCH_SIZE) -> AsyncIterator[str]:
    postgres = PostgresAsync(postgres_dsn)
    offset = 0
    try:
        while True:
            async with await postgres.get_session() as session:
                stmt = (
                    select(Domain.name)
                    .outerjoin(ARecord, ARecord.domain_id == Domain.id)
                    .where(ARecord.domain_id.is_(None))
                    .order_by(Domain.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                rows = (await session.exec(stmt)).all()

            if not rows:
                break

            for domain_name in rows:
                yield str(domain_name)

            if len(rows) < batch_size:
                break

            offset += len(rows)
    finally:
        await postgres.close()


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
            message = Message(domain.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT)
            await exchange.publish(message, routing_key=queue_name)
            sent += 1
            if sent and sent % 1000 == 0:
                log.info("Queued %d DNS jobs so far", sent)

        stop_message = Message(STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


async def count_pending_domains(postgres_dsn: str) -> int:
    postgres = PostgresAsync(postgres_dsn)
    try:
        async with await postgres.get_session() as session:
            subquery = (
                select(Domain.id)
                .outerjoin(ARecord, ARecord.domain_id == Domain.id)
                .where(ARecord.domain_id.is_(None))
                .subquery()
            )
            stmt = select(func.count()).select_from(subquery)
            result = await session.exec(stmt)
            total_row = result.one()
            total = total_row[0] if isinstance(total_row, tuple) else total_row
            return int(total or 0)
    finally:
        await postgres.close()


async def publish_pending_domains(settings: PublisherSettings) -> int:
    domains = iter_pending_domains(
        postgres_dsn=settings.postgres_dsn,
        batch_size=settings.batch_size,
    )
    return await enqueue_domains(
        rabbitmq_url=settings.rabbitmq_url,
        queue_name=settings.queue_name,
        domains=domains,
        worker_count=settings.worker_count,
        purge_queue=settings.purge_queue,
    )


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
        "Worker process %s starting (DNS records, concurrency=%d)",
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


@click.group()
def cli() -> None:
    """DNS record extraction microservice helpers."""


@cli.command("serve")
@click.option("--worker", "-w", type=int, default=4, show_default=True,
              help="Worker processes")
@click.option("--postgres-dsn", "-p", type=str, required=True,
              help="PostgreSQL DSN")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME,
              show_default=True, help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
              help="RabbitMQ prefetch per worker")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
              show_default=True, help="Concurrent DNS lookups per worker")
@click.option("--dns-timeout", type=float, default=DEFAULT_DNS_TIMEOUT,
              show_default=True, help="DNS resolver timeout in seconds")
@click.option("--log-records", is_flag=True, help="Log extracted records")
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
def serve(
    worker: int,
    postgres_dsn: str,
    rabbitmq_url: str,
    queue_name: str,
    prefetch: int,
    concurrency: int,
    dns_timeout: float,
    log_records: bool,
    verbose: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)
    base_settings = WorkerSettings(
        postgres_dsn=resolved_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=max(1, prefetch),
        concurrency=max(1, concurrency),
        dns_timeout=dns_timeout,
        log_level=log_level,
        verbose_records=log_records,
    )

    worker_args = [WorkerSettings(**base_settings.__dict__) for _ in range(max(1, worker))]
    if worker == 1:
        click.echo(run_worker(worker_args[0]))
        return

    with multiprocessing.Pool(processes=worker) as pool:
        log.info("Spawned %d worker processes (service mode)", worker)
        for result in pool.imap_unordered(run_worker, worker_args):
            click.echo(result)


@cli.command("publish")
@click.option("--postgres-dsn", "-p", type=str, required=True,
              help="PostgreSQL DSN")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME,
              show_default=True, help="RabbitMQ queue name")
@click.option("--worker-count", type=int, default=4, show_default=True,
              help="Number of worker stop signals to enqueue")
@click.option("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, show_default=True,
              help="Batch size when streaming domains from PostgreSQL")
@click.option("--purge-queue/--no-purge-queue", default=True,
              help="Purge the queue before enqueueing new jobs")
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
def publish(
    postgres_dsn: str,
    rabbitmq_url: str,
    queue_name: str,
    worker_count: int,
    batch_size: int,
    purge_queue: bool,
    verbose: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)

    total = asyncio.run(count_pending_domains(resolved_dsn))
    click.echo(f"[INFO] Found {total} domains pending DNS extraction")

    if total == 0:
        click.echo("[INFO] Nothing to enqueue")
        return

    publisher_settings = PublisherSettings(
        postgres_dsn=resolved_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        worker_count=max(1, worker_count),
        purge_queue=purge_queue,
        batch_size=batch_size,
    )

    published = asyncio.run(publish_pending_domains(publisher_settings))
    click.echo(
        f"[INFO] Published {published} DNS jobs to RabbitMQ queue '{queue_name}'"
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    CLITool(cli).run()
