#!/usr/bin/env python3
"""Distributed GeoIP enrichment microservice backed by RabbitMQ and PostgreSQL."""

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
import json
import logging
import multiprocessing
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional, Sequence

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from geoip2 import database
from geoip2.errors import AddressNotFoundError
from sqlalchemy import func
from sqlmodel import select, delete

from shared.models.postgres import Domain, GeoPoint, ARecord  # noqa: E402
from async_sqlmodel_helpers import resolve_async_dsn
from tools.dns_shared import PostgresAsync, configure_logging


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 200
DEFAULT_CONCURRENCY = 25
DEFAULT_QUEUE_NAME = "geoip_enrichment"

log = logging.getLogger("extract_geoip")


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
class GeoIPStats:
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


@dataclass
class WorkerSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    prefetch: int
    concurrency: int
    mmdb_path: Path
    log_level: int


@dataclass
class GeoIPJob:
    domain_id: int
    ips: Sequence[str]
    message: Optional[aio_pika.IncomingMessage] = None
    stop: bool = False


def _sanitize_coordinates(
    lon: Optional[float], lat: Optional[float]
) -> tuple[Optional[float], Optional[float]]:
    lon_val = round(lon, 5) if lon is not None else None
    lat_val = round(lat, 5) if lat is not None else None
    return (lon_val, lat_val)


class GeoIPRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
        self.reader = database.Reader(str(settings.mmdb_path))
        self.stats = GeoIPStats()

    async def close(self) -> None:
        self.reader.close()
        await self.postgres.close()

    async def process_job(self, job: GeoIPJob) -> bool:
        ips = [ip for ip in job.ips if ip]
        if not ips:
            await self._mark_geoip_failure_by_id(job.domain_id)
            self.stats.record(False)
            return False

        success = False
        for ip in ips:
            try:
                response = await asyncio.to_thread(self.reader.city, ip)
            except AddressNotFoundError:
                log.debug("IP not found in GeoIP database: %s", ip)
                continue
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("GeoIP lookup failed for %s: %s", ip, exc)
                continue

            longitude, latitude = _sanitize_coordinates(
                response.location.longitude, response.location.latitude
            )
            
            geo_data = {
                "country_code": response.country.iso_code,
                "country": response.country.name,
                "state": response.subdivisions.most_specific.name,
                "city": response.city.name,
                "longitude": longitude,
                "latitude": latitude,
            }

            await self._update_geo_fields(job.domain_id, geo_data)
            log.info("Updated GeoIP for %s -> %s",
                     ip, geo_data.get("country_code"))
            success = True

        if success:
            self.stats.record(True)
            return True

        await self._mark_geoip_failure_by_id(job.domain_id)
        self.stats.record(False)
        return False

    async def _update_geo_fields(
        self, domain_id: int, geo_data: Dict[str, Any]
    ) -> None:
        updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        
        async with self.postgres.get_session() as session:
            # Update domain denormalized fields
            domain_stmt = select(Domain).where(Domain.id == domain_id)
            result = await session.exec(domain_stmt)
            domain = result.one_or_none()
            
            if domain:
                domain.country_code = geo_data.get("country_code")
                domain.country = geo_data.get("country")
                domain.state = geo_data.get("state")
                domain.city = geo_data.get("city")
                domain.updated_at = updated_at
                session.add(domain)
                
                # Delete existing geo point if it exists
                await session.exec(
                    delete(GeoPoint).where(GeoPoint.domain_id == domain_id)
                )
                
                # Create new geo point
                geo_point = GeoPoint(
                    domain_id=domain_id,
                    latitude=geo_data.get("latitude"),
                    longitude=geo_data.get("longitude"),
                    country_code=geo_data.get("country_code"),
                    country=geo_data.get("country"),
                    state=geo_data.get("state"),
                    city=geo_data.get("city"),
                    updated_at=updated_at,
                )
                session.add(geo_point)
                
                await session.commit()

    async def _mark_geoip_failure_by_id(self, domain_id: int) -> None:
        updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        
        async with self.postgres.get_session() as session:
            # Just update the domain's updated_at to mark attempt
            domain_stmt = select(Domain).where(Domain.id == domain_id)
            result = await session.exec(domain_stmt)
            domain = result.one_or_none()
            
            if domain:
                domain.updated_at = updated_at
                session.add(domain)
                await session.commit()


async def get_jobs(settings: WorkerSettings) -> AsyncIterator[GeoIPJob]:
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max(1, settings.prefetch))
        queue = await channel.declare_queue(settings.queue_name, durable=True)

        async with queue.iterator() as iterator:
            async for message in iterator:
                if message.body == STOP_SENTINEL:
                    yield GeoIPJob(
                        domain_id=0, ips=[], message=message, stop=True
                    )
                    break

                try:
                    payload = json.loads(message.body.decode("utf-8"))
                    domain_id = int(payload["id"])
                    ips = payload.get("ips", [])
                except Exception as exc:  # pragma: no cover - defensive
                    log.error("Invalid GeoIP job payload: %s", exc)
                    await message.ack()
                    continue

                yield GeoIPJob(domain_id=domain_id, ips=ips, message=message)
    finally:
        await connection.close()


class GeoIPConsumer:
    def __init__(
        self, settings: WorkerSettings, runtime: GeoIPRuntime
    ) -> None:
        self.settings = settings
        self.runtime = runtime
        self._semaphore = asyncio.Semaphore(max(1, settings.concurrency))

    async def consume(self) -> None:
        tasks: set[asyncio.Task[None]] = set()

        async for job in get_jobs(self.settings):
            if job.stop:
                if job.message:
                    await job.message.ack()
                break

            task = asyncio.create_task(self._handle_job(job))
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        if tasks:
            await asyncio.gather(*tasks)

    async def _handle_job(self, job: GeoIPJob) -> None:
        async with self._semaphore:
            try:
                await self.runtime.process_job(job)
            except Exception as exc:  # pragma: no cover - defensive
                log.exception(
                    "Unhandled error while processing GeoIP job: %s", exc
                )
            finally:
                if job.message:
                    with contextlib.suppress(Exception):
                        await job.message.ack()


async def service_worker(settings: WorkerSettings) -> str:
    runtime = GeoIPRuntime(settings)
    try:
        consumer = GeoIPConsumer(settings, runtime)
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


async def count_pending_domains(postgres_dsn: str) -> int:
    """Count domains that still require GeoIP enrichment."""
    db = PostgresAsync(postgres_dsn)
    try:
        async with db.get_session() as session:
            stmt = (
                select(func.count(func.distinct(Domain.id)))
                .join(ARecord, Domain.id == ARecord.domain_id)
                .where(Domain.country_code.is_(None))
                .where(~Domain.id.in_(
                    select(GeoPoint.domain_id).where(
                        GeoPoint.domain_id.is_not(None)
                    )
                ))
            )
            result = await session.exec(stmt)
            total = result.one()
            return int(total or 0)
    finally:
        await db.close()


async def iter_pending_geoip_jobs(
    postgres_dsn: str,
    batch_size: int = 10_000,
) -> AsyncIterator[Dict[str, Any]]:
    """Yield pending GeoIP jobs in batches for RabbitMQ publication."""
    db = PostgresAsync(postgres_dsn)
    offset = 0
    try:
        while True:
            async with db.get_session() as session:
                id_stmt = (
                    select(Domain.id)
                    .join(ARecord, Domain.id == ARecord.domain_id)
                    .where(Domain.country_code.is_(None))
                    .where(~Domain.id.in_(
                        select(GeoPoint.domain_id).where(
                            GeoPoint.domain_id.is_not(None)
                        )
                    ))
                    .group_by(Domain.id)
                    .order_by(Domain.updated_at.desc())
                    .offset(offset)
                    .limit(batch_size)
                )
                domain_ids = [int(domain_id) for domain_id in (await session.exec(id_stmt)).all()]

            if not domain_ids:
                break

            async with db.get_session() as session:
                ip_stmt = (
                    select(ARecord.domain_id, ARecord.ip_address)
                    .where(ARecord.domain_id.in_(domain_ids))
                )
                rows = (await session.exec(ip_stmt)).all()

            ip_map: Dict[int, list[str]] = {}
            for domain_id, ip_address in rows:
                if ip_address:
                    ip_map.setdefault(domain_id, []).append(ip_address)

            for domain_id in domain_ids:
                ips = ip_map.get(domain_id)
                if ips:
                    yield {"id": domain_id, "ips": ips}

            if len(domain_ids) < batch_size:
                break

            offset += len(domain_ids)
    finally:
        await db.close()


async def enqueue_geoip_jobs(
    rabbitmq_url: str,
    queue_name: str,
    jobs: AsyncIterator[Dict[str, Any]],
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
            if not job.get("ips"):
                continue
            message = Message(
                json.dumps(job).encode("utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
            )
            await exchange.publish(message, routing_key=queue_name)
            sent += 1
            if sent and sent % 1000 == 0:
                log.info("Queued %d GeoIP jobs so far", sent)

        stop_message = Message(
            STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT
        )
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


@dataclass
class PublisherSettings:
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str
    worker_count: int
    purge_queue: bool
    batch_size: int = 10_000


async def publish_pending_geoip_jobs(settings: PublisherSettings) -> int:
    """Publish GeoIP jobs to RabbitMQ."""
    jobs = iter_pending_geoip_jobs(
        postgres_dsn=settings.postgres_dsn,
        batch_size=settings.batch_size,
    )
    return await enqueue_geoip_jobs(
        rabbitmq_url=settings.rabbitmq_url,
        queue_name=settings.queue_name,
        jobs=jobs,
        worker_count=settings.worker_count,
        purge_queue=settings.purge_queue,
    )


@click.group()
def cli() -> None:
    """GeoIP enrichment microservice commands."""


@cli.command("serve")
@click.option("--worker", "-w", type=int, default=4, show_default=True,
              help="Worker processes to spawn")
@click.option("--postgres-dsn", type=str, required=True,
              help="PostgreSQL DSN")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME,
              show_default=True, help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
              help="RabbitMQ prefetch per worker")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
              show_default=True, help="Concurrent GeoIP lookups per worker")
@click.option("--mmdb-path", type=Path, required=True,
              help="Path to the MaxMind GeoIP2/City database")
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
def serve(
    worker: int,
    postgres_dsn: str,
    rabbitmq_url: str,
    queue_name: str,
    prefetch: int,
    concurrency: int,
    mmdb_path: Path,
    verbose: bool,
) -> None:
    """Run the GeoIP worker microservice."""

    if not mmdb_path.exists():
        raise click.BadParameter(f"GeoIP database not found: {mmdb_path}")

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
        mmdb_path=mmdb_path,
        log_level=log_level,
    )

    worker_args = [WorkerSettings(**base_settings.__dict__) for _ in range(worker)]
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
@click.option("--batch-size", type=int, default=10_000, show_default=True,
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
    """Publish pending GeoIP jobs onto RabbitMQ."""

    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)

    total = asyncio.run(count_pending_domains(resolved_dsn))
    click.echo(f"[INFO] Found {total} domains pending GeoIP enrichment")

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

    published = asyncio.run(publish_pending_geoip_jobs(publisher_settings))
    click.echo(
        f"[INFO] Published {published} GeoIP jobs to RabbitMQ queue '{queue_name}'"
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    CLITool(cli).run()
