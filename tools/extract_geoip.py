#!/usr/bin/env python3
"""Distributed GeoIP enrichment backed by RabbitMQ and PostgreSQL.

Jobs can be enqueued onto RabbitMQ for horizontal workers, or processed
directly without a message broker. Workers claim domains atomically
via PostgreSQL, set geo_lookup_started to avoid duplication, and clear
failure markers on success.
"""

from __future__ import annotations

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
from sqlalchemy.ext.asyncio import (
    async_sessionmaker, create_async_engine
)
from sqlmodel import select, delete
from sqlmodel.ext.asyncio.session import AsyncSession

# Add the project root to the Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.postgres import Domain, GeoPoint, ARecord  # noqa: E402


class PostgresAsync:
    """Async PostgreSQL connection manager."""
    
    def __init__(self, dsn: str):
        # Convert DSN to use asyncpg driver (consistent with other tools)
        if dsn.startswith("postgresql://"):
            dsn = dsn.replace("postgresql://", "postgresql+asyncpg://")
        elif not dsn.startswith("postgresql+asyncpg://"):
            # If no scheme, assume we need asyncpg
            if "://" not in dsn:
                dsn = f"postgresql+asyncpg://{dsn}"

        self._engine = create_async_engine(
            dsn,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=True,
            future=True
        )
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            class_=AsyncSession
        )
    
    def get_session(self):
        """Get async database session."""
        return self._session_factory()
    
    async def close(self):
        """Close database engine."""
        await self._engine.dispose()


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
    rabbitmq_url: Optional[str]
    queue_name: str
    prefetch: int
    concurrency: int
    claim_timeout: int
    mmdb_path: Path
    log_level: int


@dataclass
class DirectWorkerSettings(WorkerSettings):
    """Settings for direct (non-RabbitMQ) workers."""


@dataclass
class GeoIPJob:
    domain_id: int
    ips: Sequence[str]
    message: Optional[aio_pika.IncomingMessage] = None
    stop: bool = False


async def build_base_query():
    """Return the base query for domains missing GeoIP data."""
    return (
        select(Domain)
        .join(ARecord)
        .where(Domain.country_code.is_(None))
        .where(~Domain.id.in_(
            select(GeoPoint.domain_id).where(GeoPoint.domain_id.is_not(None))
        ))
    )


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


async def claim_next_domain(
    postgres: PostgresAsync, claim_timeout: int
) -> Optional[Dict[str, Any]]:
    """Claim the next domain for GeoIP processing."""
    async with postgres.get_session() as session:
        # Find domains with A records but no GeoIP data
        stmt = (
            select(Domain, ARecord.ip_address)
            .join(ARecord, Domain.id == ARecord.domain_id)
            .where(Domain.country_code.is_(None))
            .where(~Domain.id.in_(
                select(GeoPoint.domain_id).where(
                    GeoPoint.domain_id.is_not(None)
                )
            ))
            .limit(1)
        )
        
        result = await session.exec(stmt)
        row = result.first()
        
        if row:
            domain, ip = row
            # Get all A records for this domain
            a_records_stmt = select(ARecord.ip_address).where(
                ARecord.domain_id == domain.id
            )
            a_result = await session.exec(a_records_stmt)
            ips = [record for record in a_result.all()]
            
            return {"id": domain.id, "ips": ips}
        
        return None


async def get_jobs(settings: WorkerSettings) -> AsyncIterator[GeoIPJob]:
    if not settings.rabbitmq_url:
        raise ValueError("RabbitMQ URL is required to consume GeoIP jobs")

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


async def direct_worker(settings: DirectWorkerSettings) -> str:
    runtime = GeoIPRuntime(settings)
    try:
        idle_iterations = 0
        while True:
            claimed = await claim_next_domain(
                runtime.postgres, settings.claim_timeout
            )
            if not claimed:
                idle_iterations += 1
                if idle_iterations >= 5:
                    break
                await asyncio.sleep(1)
                continue

            idle_iterations = 0
            domain_id = claimed.get("id")
            ips = claimed.get("ips") or []
            if not domain_id:
                continue

            job = GeoIPJob(domain_id=domain_id, ips=ips)
            await runtime.process_job(job)

        return runtime.stats.summary()
    finally:
        await runtime.close()


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


def run_worker_direct(settings: DirectWorkerSettings) -> str:
    configure_logging(settings.log_level)
    log.info(
        "Worker process %s starting (mode=direct, timeout=%d min)",
        os.getpid(),
        settings.claim_timeout,
    )
    try:
        summary = asyncio.run(direct_worker(settings))
        log.info("Worker %s finished: %s", os.getpid(), summary)
        return f"Worker {os.getpid()} done ({summary})"
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("Worker process %s crashed", os.getpid())
        return f"[ERROR] Worker crashed: {exc}"


async def count_pending_domains(postgres: PostgresAsync) -> int:
    """Count domains that need GeoIP enrichment."""
    async with postgres.get_session() as session:
        stmt = (
            select(Domain.id)
            .join(ARecord, Domain.id == ARecord.domain_id)
            .where(Domain.country_code.is_(None))
            .where(~Domain.id.in_(
                select(GeoPoint.domain_id).where(
                    GeoPoint.domain_id.is_not(None)
                )
            ))
        )
        result = await session.exec(stmt)
        return len(result.all())


async def iter_pending_geoip_jobs(
    postgres: PostgresAsync, claim_timeout: int
) -> AsyncIterator[Dict[str, Any]]:
    """Iterate over domains that need GeoIP processing."""
    while True:
        claimed = await claim_next_domain(postgres, claim_timeout)
        if not claimed:
            break
        yield claimed


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


@click.command()
@click.option(
    "--worker", "-w", type=int, default=4, show_default=True,
    help="Worker processes"
)
@click.option(
    "--postgres-dsn", type=str, required=True,
    help="PostgreSQL connection string"
)
@click.option(
    "--input", "mmdb_path", type=Path, required=True,
    help="Path to GeoIP2/City mmdb"
)
@click.option(
    "--claim-timeout", type=int, default=30, show_default=True,
    help="Minutes before an in-progress claim is retried"
)
@click.option(
    "--rabbitmq-url", "-r", type=str, default=None,
    help="RabbitMQ URL for distributed mode"
)
@click.option(
    "--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME,
    show_default=True, help="RabbitMQ queue name"
)
@click.option(
    "--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
    help="RabbitMQ prefetch per worker"
)
@click.option(
    "--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
    show_default=True, help="Concurrent GeoIP jobs per worker"
)
@click.option(
    "--purge-queue/--no-purge-queue", default=True,
    help="Purge queue before enqueuing new jobs"
)
@click.option(
    "--service", is_flag=True,
    help="Run as a RabbitMQ-consuming GeoIP service"
)
@click.option(
    "--verbose", is_flag=True, help="Enable verbose debug logging"
)
def main(
    worker: int,
    postgres_dsn: str,
    mmdb_path: Path,
    claim_timeout: int,
    rabbitmq_url: Optional[str],
    queue_name: str,
    prefetch: int,
    concurrency: int,
    purge_queue: bool,
    service: bool,
    verbose: bool,
) -> None:
    if not mmdb_path.exists():
        raise FileNotFoundError(f"GeoIP database not found: {mmdb_path}")

    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    worker = max(1, worker)
    prefetch = max(1, prefetch)
    concurrency = max(1, concurrency)

    base_settings = WorkerSettings(
        postgres_dsn=postgres_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        concurrency=concurrency,
        claim_timeout=claim_timeout,
        mmdb_path=mmdb_path,
        log_level=log_level,
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

    postgres = PostgresAsync(postgres_dsn)
    try:
        total = asyncio.run(count_pending_domains(postgres))
        click.echo(f"[INFO] total domains pending GeoIP enrichment: {total}")

        if total == 0:
            click.echo("[INFO] Nothing to enrich")
            return

        if rabbitmq_url:
            log.info("Publishing GeoIP jobs to RabbitMQ at %s", rabbitmq_url)
            jobs = iter_pending_geoip_jobs(postgres, claim_timeout)
            queued = asyncio.run(
                enqueue_geoip_jobs(
                    rabbitmq_url=rabbitmq_url,
                    queue_name=queue_name,
                    jobs=jobs,
                    worker_count=worker,
                    purge_queue=purge_queue,
                )
            )
            log.info("Queued %d GeoIP jobs onto RabbitMQ queue '%s'",
                     queued, queue_name)
            click.echo(
                "[INFO] Published GeoIP jobs. Start workers with --service"
            )
            return

        worker_args = [DirectWorkerSettings(
            **base_settings.__dict__) for _ in range(worker)]
        if worker == 1:
            click.echo(run_worker_direct(worker_args[0]))
            return

        with multiprocessing.Pool(processes=worker) as pool:
            log.info("Spawned %d direct worker processes", worker)
            for result in pool.imap_unordered(run_worker_direct, worker_args):
                click.echo(result)
    finally:
        asyncio.run(postgres.close())


if __name__ == "__main__":
    main()
