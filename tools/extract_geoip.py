#!/usr/bin/env python3
"""Distributed GeoIP enrichment backed by RabbitMQ and MongoDB.

This refactor aligns the GeoIP enrichment workflow with other distributed
scanners (e.g. ``ssl_cert_scanner.py``). Jobs can be enqueued onto RabbitMQ for
horizontal workers, or processed directly without a message broker. Workers
claim documents atomically via Mongo, set ``geo_lookup_started`` to avoid
duplication, and clear failure markers on success.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import multiprocessing
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Sequence

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from bson import ObjectId
from geoip2 import database
from geoip2.errors import AddressNotFoundError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection

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
    mongo_host: str
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
    document_id: ObjectId
    ips: Sequence[str]
    message: Optional[aio_pika.IncomingMessage] = None
    stop: bool = False


def build_query() -> Dict[str, Any]:
    """Return the base query for documents missing GeoIP data."""

    return {
        "a_record.0": {"$exists": True},
        "country_code": {"$exists": False},
        "geo_lookup_failed": {"$exists": False},
    }


def _sanitize_coordinates(lon: Optional[float], lat: Optional[float]) -> List[Optional[float]]:
    lon_val = round(lon, 5) if lon is not None else None
    lat_val = round(lat, 5) if lat is not None else None
    return [lon_val, lat_val]


class GeoIPRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self._client = AsyncIOMotorClient(
            f"mongodb://{settings.mongo_host}:27017", tz_aware=True)
        self.db = self._client.ip_data
        self.collection = self.db.dns
        self.reader = database.Reader(str(settings.mmdb_path))
        self.stats = GeoIPStats()

    async def close(self) -> None:
        self.reader.close()
        self._client.close()

    async def process_job(self, job: GeoIPJob) -> bool:
        ips = [ip for ip in job.ips if ip]
        if not ips:
            await self._mark_geoip_failure_by_id(job.document_id)
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

            post = {
                "country_code": response.country.iso_code,
                "country": response.country.name,
                "state": response.subdivisions.most_specific.name,
                "city": response.city.name,
                "loc": {
                    "type": "Point",
                    "coordinates": _sanitize_coordinates(
                        response.location.longitude, response.location.latitude
                    ),
                },
            }

            await self._update_geo_fields(job.document_id, post)
            log.info("Updated GeoIP for %s -> %s",
                     ip, post.get("country_code"))
            success = True

        if success:
            self.stats.record(True)
            return True

        await self._mark_geoip_failure_by_id(job.document_id)
        self.stats.record(False)
        return False

    async def _update_geo_fields(self, doc_id: ObjectId, post: Dict[str, Any]) -> None:
        updated = datetime.now(timezone.utc)
        set_fields: Dict[str, Any] = {
            "geo": post,
            "country_code": post.get("country_code"),
            "country": post.get("country"),
            "state": post.get("state"),
            "city": post.get("city"),
            "loc": post.get("loc"),
            "updated": updated,
        }

        await self.collection.update_one(
            {"_id": doc_id},
            {
                "$set": set_fields,
                "$unset": {"geo_lookup_failed": "", "geo_lookup_started": ""},
            },
            upsert=False,
        )

    async def _mark_geoip_failure_by_id(self, doc_id: ObjectId) -> None:
        await self.collection.update_one(
            {"_id": doc_id},
            {
                "$set": {"geo_lookup_failed": datetime.now(timezone.utc)},
                "$unset": {"geo_lookup_started": ""},
            },
            upsert=False,
        )


async def claim_next_document(collection, claim_timeout: int) -> Optional[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    base_query = build_query()

    if claim_timeout:
        expire_before = now - timedelta(minutes=claim_timeout)
        claim_clause: Dict[str, Any] = {
            "$or": [
                {"geo_lookup_started": {"$exists": False}},
                {"geo_lookup_started": {"$lt": expire_before}},
            ]
        }
    else:
        claim_clause = {"geo_lookup_started": {"$exists": False}}

    query = {"$and": [base_query, claim_clause]}

    return await collection.find_one_and_update(
        query,
        {"$set": {"geo_lookup_started": now}},
        sort=[("_id", 1)],
        projection={"a_record": 1},
        return_document=ReturnDocument.BEFORE,
    )


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
                    yield GeoIPJob(document_id=ObjectId(), ips=[], message=message, stop=True)
                    break

                try:
                    payload = json.loads(message.body.decode("utf-8"))
                    doc_id = ObjectId(payload["id"])
                    ips = payload.get("ips", [])
                except Exception as exc:  # pragma: no cover - defensive
                    log.error("Invalid GeoIP job payload: %s", exc)
                    await message.ack()
                    continue

                yield GeoIPJob(document_id=doc_id, ips=ips, message=message)
    finally:
        await connection.close()


class GeoIPConsumer:
    def __init__(self, settings: WorkerSettings, runtime: GeoIPRuntime) -> None:
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
                    "Unhandled error while processing GeoIP job: %s", exc)
            finally:
                if job.message:
                    with contextlib.suppress(Exception):
                        await job.message.ack()


async def direct_worker(settings: DirectWorkerSettings) -> str:
    runtime = GeoIPRuntime(settings)
    try:
        idle_iterations = 0
        while True:
            claimed = await claim_next_document(runtime.collection, settings.claim_timeout)
            if not claimed:
                idle_iterations += 1
                if idle_iterations >= 5:
                    break
                await asyncio.sleep(1)
                continue

            idle_iterations = 0
            doc_id = claimed.get("_id")
            ips = claimed.get("a_record") or []
            if not doc_id:
                continue

            job = GeoIPJob(document_id=doc_id, ips=ips)
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


def claim_next_document_sync(collection: Collection, claim_timeout: int) -> Optional[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    base_query = build_query()

    if claim_timeout:
        expire_before = now - timedelta(minutes=claim_timeout)
        claim_clause: Dict[str, Any] = {
            "$or": [
                {"geo_lookup_started": {"$exists": False}},
                {"geo_lookup_started": {"$lt": expire_before}},
            ]
        }
    else:
        claim_clause = {"geo_lookup_started": {"$exists": False}}

    query = {"$and": [base_query, claim_clause]}

    return collection.find_one_and_update(
        query,
        {"$set": {"geo_lookup_started": now}},
        sort=[("_id", 1)],
        projection={"a_record": 1},
        return_document=ReturnDocument.BEFORE,
    )


def mark_geoip_failure_by_id_sync(collection: Collection, doc_id: Any) -> None:
    collection.update_one(
        {"_id": doc_id},
        {
            "$set": {"geo_lookup_failed": datetime.now(timezone.utc)},
            "$unset": {"geo_lookup_started": ""},
        },
        upsert=False,
    )


def iter_pending_geoip_jobs(collection: Collection, claim_timeout: int) -> Iterable[Dict[str, Any]]:
    while True:
        claimed = claim_next_document_sync(collection, claim_timeout)
        if not claimed:
            break

        doc_id = claimed.get("_id")
        ips = claimed.get("a_record") or []
        if not doc_id or not ips:
            if doc_id is not None:
                mark_geoip_failure_by_id_sync(collection, doc_id)
            continue

        yield {"id": str(doc_id), "ips": [ip for ip in ips if ip]}


def count_pending_documents(collection: Collection) -> int:
    return collection.count_documents(build_query())


async def enqueue_geoip_jobs(
    rabbitmq_url: str,
    queue_name: str,
    jobs: Iterable[Dict[str, Any]],
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
        for job in jobs:
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
            STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


@click.command()
@click.option("--worker", "-w", type=int, default=4, show_default=True, help="Worker processes")
@click.option("--host", "-h", type=str, required=True, help="MongoDB host")
@click.option("--input", "mmdb_path", type=Path, required=True, help="Path to GeoIP2/City mmdb")
@click.option("--claim-timeout", type=int, default=30, show_default=True,
              help="Minutes before an in-progress claim is retried")
@click.option("--rabbitmq-url", "-r", type=str, default=None, help="RabbitMQ URL for distributed mode")
@click.option("--queue-name", "-q", type=str, default=DEFAULT_QUEUE_NAME, show_default=True,
              help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
              help="RabbitMQ prefetch per worker")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY, show_default=True,
              help="Concurrent GeoIP jobs per worker")
@click.option("--purge-queue/--no-purge-queue", default=True,
              help="Purge queue before enqueuing new jobs")
@click.option("--service", is_flag=True, help="Run as a RabbitMQ-consuming GeoIP service")
@click.option("--verbose", is_flag=True, help="Enable verbose debug logging")
def main(
    worker: int,
    host: str,
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
        mongo_host=host,
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

    sync_client = MongoClient(f"mongodb://{host}:27017", tz_aware=True)
    collection = sync_client.ip_data.dns
    total = count_pending_documents(collection)
    click.echo(f"[INFO] total documents pending GeoIP enrichment: {total}")

    if total == 0:
        sync_client.close()
        click.echo("[INFO] Nothing to enrich")
        return

    if rabbitmq_url:
        jobs = iter_pending_geoip_jobs(collection, claim_timeout)
        log.info("Publishing GeoIP jobs to RabbitMQ at %s", rabbitmq_url)
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
        sync_client.close()
        click.echo("[INFO] Published GeoIP jobs. Start workers with --service")
        return

    sync_client.close()
    worker_args = [DirectWorkerSettings(
        **base_settings.__dict__) for _ in range(worker)]
    if worker == 1:
        click.echo(run_worker_direct(worker_args[0]))
        return

    with multiprocessing.Pool(processes=worker) as pool:
        log.info("Spawned %d direct worker processes", worker)
        for result in pool.imap_unordered(run_worker_direct, worker_args):
            click.echo(result)


if __name__ == "__main__":
    main()
