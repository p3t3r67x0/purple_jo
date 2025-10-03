#!/usr/bin/env python3
"""RabbitMQ-backed domain extractor that normalises URLs into Domain records."""

from __future__ import annotations

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

import asyncio
import json
import logging
import multiprocessing
import os
import re
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import AsyncIterator, Dict, Optional

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from async_sqlmodel_helpers import resolve_async_dsn
from shared.models.postgres import Domain, Url
from tools.dns_shared import PostgresAsync, configure_logging

STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 200
DEFAULT_CONCURRENCY = 250
DEFAULT_QUEUE_NAME = "extract_domains"
DEFAULT_BATCH_SIZE = 10_000

log = logging.getLogger("extract_domains")


def utcnow() -> datetime:
    """Return current UTC datetime without timezone info."""
    return datetime.now(UTC).replace(tzinfo=None)


def is_ipv4(candidate: str) -> bool:
    return bool(re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", candidate))


def find_domain(value: str) -> Optional[str]:
    match = re.search(
        r"([\w\-.]{1,63}|[\w\-.]{1,63}[^\x00-\x7F\w-]{1,63})\.([\w\-.]{2,})|"
        r"(([\w\d-]{1,63}|[\d\w-]*[^\x00-\x7F\w-]{1,63}))\.?"
        r"([\w\d]{1,63}|[\d\w\-.]*[^\x00-\x7F\-.]{1,63})\."
        r"([a-z\.]{2,}|[\w]*[^\x00-\x7F\.]{2,})",
        value,
    )
    return match.group(0) if match else None


@dataclass
class WorkerSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    prefetch: int
    concurrency: int
    log_level: int
    verbose_urls: bool


@dataclass
class PublisherSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    worker_count: int
    purge_queue: bool
    batch_size: int = DEFAULT_BATCH_SIZE


@dataclass
class DomainJob:
    url_id: int
    url: str
    message: aio_pika.IncomingMessage
    stop: bool = False


@dataclass
class DomainStats:
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
        return f"processed={self.processed} succeeded={self.succeeded} failed={self.failed}"


class DomainRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
        self.stats = DomainStats()

    async def close(self) -> None:
        await self.postgres.close()

    async def process_job(self, url_id: int, url_value: str) -> bool:
        domain = find_domain(url_value) or ""
        success = False
        try:
            success = await self._store_result(url_id, domain)
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("Failed processing URL %s (id=%s): %s", url_value, url_id, exc)
        self.stats.record(success)
        if success and self.settings.verbose_urls:
            log.info("Extracted domain %s from %s", domain.lower(), url_value)
        elif not success:
            log.warning("No usable domain extracted from %s", url_value)
        return success

    async def _store_result(self, url_id: int, domain: str) -> bool:
        domain = domain.strip().lower()
        async with self.postgres.get_session() as session:
            now = utcnow()

            url_obj = await session.get(Url, url_id)
            if not url_obj:
                return False

            url_obj.domain_extracted = now
            session.add(url_obj)

            if domain and not is_ipv4(domain):
                stmt = (
                    insert(Domain)
                    .values(name=domain, created_at=now, updated_at=now)
                    .on_conflict_do_update(
                        index_elements=[Domain.__table__.c.name],
                        set_={"updated_at": now},
                    )
                )
                await session.exec(stmt)
                await session.commit()
                return True

            await session.commit()
            return False


class DomainConsumer:
    def __init__(self, settings: WorkerSettings, runtime: DomainRuntime) -> None:
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
                        log.debug("Received STOP sentinel, consumer exiting")
                        break

                    job = json.loads(message.body.decode("utf-8"))
                    url_id = int(job.get("url_id"))
                    url_value = str(job.get("url", ""))

                    try:
                        await self.runtime.process_job(url_id, url_value)
                    finally:
                        await message.ack()
        finally:
            await connection.close()


async def iter_pending_urls(
    postgres_dsn: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> AsyncIterator[Dict[str, object]]:
    postgres = PostgresAsync(postgres_dsn)
    offset = 0
    try:
        while True:
            async with postgres.get_session() as session:
                stmt = (
                    select(Url.id, Url.url)
                    .where(Url.domain_extracted.is_(None))
                    .order_by(Url.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                rows = (await session.exec(stmt)).all()

            if not rows:
                break

            for url_id, url_value in rows:
                yield {"url_id": int(url_id), "url": str(url_value or "")}

            if len(rows) < batch_size:
                break

            offset += len(rows)
    finally:
        await postgres.close()


async def count_pending_urls(postgres_dsn: str) -> int:
    postgres = PostgresAsync(postgres_dsn)
    try:
        async with postgres.get_session() as session:
            stmt = select(func.count()).select_from(
                select(Url.id).where(Url.domain_extracted.is_(None)).subquery()
            )
            result = await session.exec(stmt)
            total_row = result.one()
            total = total_row[0]
            return int(total or 0)
    finally:
        await postgres.close()


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
                log.info("Queued %d domain extraction jobs so far", sent)

        stop_message = Message(STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


async def publish_pending_jobs(settings: PublisherSettings) -> int:
    jobs = iter_pending_urls(
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


async def service_worker(settings: WorkerSettings) -> str:
    runtime = DomainRuntime(settings)
    try:
        consumer = DomainConsumer(settings, runtime)
        await consumer.consume()
        return runtime.stats.summary()
    finally:
        await runtime.close()


def run_worker(settings: WorkerSettings) -> str:
    configure_logging(settings.log_level)
    log.info(
        "Worker process %s starting (domain extraction, concurrency=%d)",
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
    """Domain extraction microservice helpers."""


@cli.command("serve")
@click.option("--worker", "-w", type=int, default=4, show_default=True,
              help="Worker processes")
@click.option("--postgres-dsn", type=str, required=True,
              help="PostgreSQL DSN")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME,
              show_default=True, help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
              help="RabbitMQ prefetch per worker")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
              show_default=True, help="Concurrent URL extractions per worker")
@click.option("--verbose-urls", is_flag=True, help="Log each extracted domain")
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
def serve(
    worker: int,
    postgres_dsn: str,
    rabbitmq_url: str,
    queue_name: str,
    prefetch: int,
    concurrency: int,
    verbose_urls: bool,
    verbose: bool,
) -> None:
    """Run domain extraction workers consuming RabbitMQ jobs."""

    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)
    base_settings = WorkerSettings(
        postgres_dsn=resolved_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=max(1, prefetch),
        concurrency=max(1, concurrency),
        log_level=log_level,
        verbose_urls=verbose_urls,
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
@click.option("--postgres-dsn", type=str, required=True,
              help="PostgreSQL DSN")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME,
              show_default=True, help="RabbitMQ queue name")
@click.option("--worker-count", type=int, default=4, show_default=True,
              help="Number of worker stop signals to enqueue")
@click.option("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, show_default=True,
              help="Batch size when streaming URLs from PostgreSQL")
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
    """Publish pending URL records for domain extraction."""

    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)

    total = asyncio.run(count_pending_urls(resolved_dsn))
    click.echo(f"[INFO] Found {total} URLs pending domain extraction")

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
        f"[INFO] Published {published} domain extraction jobs to RabbitMQ queue '{queue_name}'"
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    CLITool(cli).run()
