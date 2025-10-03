#!/usr/bin/env python3
"""RabbitMQ-based WHOIS enrichment pipeline backed by PostgreSQL."""

from __future__ import annotations
from typing import AsyncIterator, Dict, List, Optional
from datetime import datetime, timezone
from dataclasses import dataclass
import os
import multiprocessing
import logging
import json
import asyncio

from importlib import import_module

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

try:
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

bootstrap.setup()

from tools.dns_shared import PostgresAsync, configure_logging
from async_sqlmodel_helpers import resolve_async_dsn
from shared.models.postgres import ARecord, Domain, WhoisRecord
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import Select
from sqlalchemy import func, select
from ipwhois.net import Net
from ipwhois.asn import IPASN
import click
from aiormq.exceptions import AMQPConnectionError
from aio_pika import DeliveryMode, Message
import aio_pika


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 200
DEFAULT_WORKER_CONCURRENCY = 10
DEFAULT_QUEUE_NAME = "whois_enrichment"
DEFAULT_BATCH_SIZE = 10_000

log = logging.getLogger("extract_whois")


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with PostgreSQL columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class WorkerSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    prefetch: int
    concurrency: int
    log_level: int


@dataclass
class PublisherSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    worker_count: int
    purge_queue: bool
    batch_size: int = DEFAULT_BATCH_SIZE


@dataclass
class WhoisJob:
    domain_id: int
    ips: List[str]
    message: aio_pika.IncomingMessage
    stop: bool = False


@dataclass
class WhoisStats:
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


@click.group()
def cli() -> None:
    """WHOIS enrichment microservice helpers."""


def _pending_domains_select() -> Select:
    subquery = select(WhoisRecord.domain_id)
    return (
        select(Domain.id)
        .distinct()
        .join(ARecord, ARecord.domain_id == Domain.id)
        .where(~Domain.id.in_(subquery))
    )


async def iter_pending_jobs(
    postgres_dsn: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> AsyncIterator[Dict[str, object]]:
    """Yield WHOIS jobs as dictionaries with domain ID and IP list."""
    db = PostgresAsync(postgres_dsn)
    offset = 0
    try:
        while True:
            async with db.get_session() as session:
                stmt = (
                    _pending_domains_select()
                    .order_by(Domain.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                domain_rows = (await session.exec(stmt)).scalars().all()

            if not domain_rows:
                break

            async with db.get_session() as session:
                ip_stmt = (
                    select(ARecord.domain_id, ARecord.ip_address)
                    .where(ARecord.domain_id.in_(domain_rows))
                )
                ip_rows = await session.exec(ip_stmt)
                ip_map: Dict[int, List[str]] = {}
                for domain_id, ip in ip_rows.all():
                    if ip:
                        ip_map.setdefault(int(domain_id), []).append(str(ip))

            for domain_id in domain_rows:
                ips = ip_map.get(int(domain_id), [])
                if not ips:
                    continue
                yield {"domain_id": int(domain_id), "ips": ips}

            if len(domain_rows) < batch_size:
                break

            offset += len(domain_rows)
    finally:
        await db.close()


async def count_pending_domains(postgres_dsn: str) -> int:
    db = PostgresAsync(postgres_dsn)
    try:
        async with db.get_session() as session:
            stmt = select(func.count()).select_from(
                _pending_domains_select().subquery()
            )
            total = (await session.exec(stmt)).scalar()
            return int(total or 0)
    finally:
        await db.close()


async def enqueue_jobs(
    rabbitmq_url: str,
    queue_name: str,
    jobs: AsyncIterator[Dict[str, object]],
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

        async for job in jobs:
            message = Message(
                json.dumps(job).encode("utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
            )
            await exchange.publish(message, routing_key=queue_name)
            sent += 1
            if sent and sent % 1000 == 0:
                log.info("Queued %d WHOIS jobs so far", sent)

        stop_message = Message(
            STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


async def publish_pending_jobs(settings: PublisherSettings) -> int:
    jobs = iter_pending_jobs(
        postgres_dsn=settings.postgres_dsn,
        batch_size=settings.batch_size,
    )
    return await enqueue_jobs(
        rabbitmq_url=settings.rabbitmq_url,
        queue_name=settings.queue_name,
        jobs=jobs,
        worker_count=settings.worker_count,
        purge_queue=settings.purge_queue,
    )


def _perform_whois(ip: str) -> Optional[Dict[str, object]]:
    try:
        return IPASN(Net(ip)).lookup(retry_count=0, asn_methods=["whois"])
    except Exception:
        return None


class WhoisRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
        self.stats = WhoisStats()
        self._semaphore = asyncio.Semaphore(max(1, settings.concurrency))

    async def close(self) -> None:
        await self.postgres.close()

    async def process_job(self, domain_id: int, ips: List[str]) -> bool:
        loop = asyncio.get_running_loop()
        whois_data: Optional[Dict[str, object]] = None
        for ip in ips:
            whois_data = await loop.run_in_executor(None, _perform_whois, ip)
            if whois_data:
                break

        await self._store_result(domain_id, whois_data)
        success = whois_data is not None
        self.stats.record(success)
        if success:
            log.info("WHOIS lookup succeeded for domain %s", domain_id)
        else:
            log.warning(
                "WHOIS lookup returned no data for domain %s", domain_id)
        return success

    async def _store_result(
        self,
        domain_id: int,
        whois_data: Optional[Dict[str, object]],
    ) -> None:
        async with self.postgres.get_session() as session:
            values = {
                "domain_id": domain_id,
                "asn": None,
                "asn_description": None,
                "asn_country_code": None,
                "asn_registry": None,
                "asn_cidr": None,
                "updated_at": utcnow(),
            }

            if whois_data:
                values.update(
                    {
                        "asn": whois_data.get("asn"),
                        "asn_description": (whois_data.get("asn_description") or "")[:255],
                        "asn_country_code": (whois_data.get("asn_country_code") or "")[:8],
                        "asn_registry": (whois_data.get("asn_registry") or "")[:255],
                        "asn_cidr": (whois_data.get("asn_cidr") or "")[:64],
                    }
                )

            stmt = insert(WhoisRecord).values(**values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[WhoisRecord.__table__.c.domain_id],
                set_={
                    "asn": stmt.excluded.asn,
                    "asn_description": stmt.excluded.asn_description,
                    "asn_country_code": stmt.excluded.asn_country_code,
                    "asn_registry": stmt.excluded.asn_registry,
                    "asn_cidr": stmt.excluded.asn_cidr,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await session.exec(stmt)
            await session.commit()


class WhoisConsumer:
    def __init__(self, settings: WorkerSettings, runtime: WhoisRuntime) -> None:
        self.settings = settings
        self.runtime = runtime

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
                        log.debug(
                            "Received STOP sentinel, shutting down consumer")
                        break

                    job = json.loads(message.body.decode("utf-8"))
                    domain_id = int(job.get("domain_id"))
                    ips = [str(ip) for ip in job.get("ips", []) if ip]

                    if not ips:
                        await message.ack()
                        continue

                    async with self.runtime._semaphore:
                        try:
                            await self.runtime.process_job(domain_id, ips)
                        except Exception as exc:  # pragma: no cover - defensive
                            log.exception(
                                "WHOIS job failed for domain %s: %s", domain_id, exc)
                        finally:
                            await message.ack()
        finally:
            await connection.close()


async def service_worker(settings: WorkerSettings) -> str:
    runtime = WhoisRuntime(settings)
    try:
        consumer = WhoisConsumer(settings, runtime)
        await consumer.consume()
        return runtime.stats.summary()
    finally:
        await runtime.close()


def run_worker(settings: WorkerSettings) -> str:
    configure_logging(settings.log_level)
    log.info(
        "Worker process %s starting (WHOIS, concurrency=%d)",
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


@cli.command("serve")
@click.option("--worker", "-w", type=int, default=4, show_default=True,
              help="Worker processes")
@click.option("--postgres-dsn", type=str, required=True,
              help="PostgreSQL DSN")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME,
              show_default=True, help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH,
              show_default=True, help="RabbitMQ prefetch per worker")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_WORKER_CONCURRENCY,
              show_default=True, help="Simultaneous WHOIS lookups per worker task")
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
def serve(
    worker: int,
    postgres_dsn: str,
    rabbitmq_url: str,
    queue_name: str,
    prefetch: int,
    concurrency: int,
    verbose: bool,
) -> None:
    """Run WHOIS enrichment workers consuming from RabbitMQ."""

    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)
    worker = max(1, worker)
    prefetch = max(1, prefetch)
    concurrency = max(1, concurrency)

    base_settings = WorkerSettings(
        postgres_dsn=resolved_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        concurrency=concurrency,
        log_level=log_level,
    )

    worker_args = [WorkerSettings(**base_settings.__dict__)
                   for _ in range(worker)]
    if worker == 1:
        click.echo(run_worker(worker_args[0]))
        return

    with multiprocessing.Pool(processes=worker) as pool:
        log.info("Spawned %d worker processes (service mode)", worker)
        for result in pool.imap_unordered(run_worker, worker_args):
            click.echo(result)


@cli.command("publish")
@click.option("--postgres-dsn", type=str, required=True,
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
    """Publish pending WHOIS jobs onto RabbitMQ."""

    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)

    total = asyncio.run(count_pending_domains(resolved_dsn))
    click.echo(f"[INFO] Found {total} domains pending WHOIS enrichment")

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

    published = asyncio.run(publish_pending_jobs(publisher_settings))
    click.echo(
        f"[INFO] Published {published} WHOIS jobs to RabbitMQ queue '{queue_name}'"
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    CLITool(cli).run()
