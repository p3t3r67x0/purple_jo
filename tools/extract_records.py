#!/usr/bin/env python3
"""Distributed DNS record extractor backed by RabbitMQ and MongoDB.

This refactor moves the legacy threaded worker into a service architecture. The
script can enqueue pending domains onto a RabbitMQ queue and spin up multiple
horizontal workers which resolve DNS records concurrently. Each worker keeps a
shared async MongoDB client and executes DNS lookups in parallel using
`asyncio.to_thread`, minimising per-domain latency while keeping resource usage
predictable.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import multiprocessing
import os
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from dns import resolver
from dns.exception import DNSException, Timeout
from dns.name import EmptyLabel, LabelTooLong
from dns.resolver import NoAnswer, NoNameservers, NXDOMAIN
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 400
DEFAULT_CONCURRENCY = 200
DEFAULT_TIMEOUT = 1.5

DNS_RECORD_TYPES: Tuple[Tuple[str, str], ...] = (
    ("A", "a_record"),
    ("AAAA", "aaaa_record"),
    ("NS", "ns_record"),
    ("MX", "mx_record"),
    ("SOA", "soa_record"),
    ("CNAME", "cname_record"),
)


log = logging.getLogger("extract_records")


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
    mongo_host: str
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


class MongoAsync:
    def __init__(self, host: str) -> None:
        self._client = AsyncIOMotorClient(f"mongodb://{host}:27017", tz_aware=True)
        self.db = self._client.ip_data

    def close(self) -> None:
        self._client.close()


class DNSRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.mongo = MongoAsync(settings.mongo_host)
        self.stats = DNSStats()

    async def close(self) -> None:
        self.mongo.close()

    async def process_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain:
            return False

        records = await self._resolve_all(domain)
        now = datetime.now()

        if records:
            await self._store_records(domain, records, now)
            self._log_records(domain, records)
            self.stats.record(True)
            return True

        await self._mark_failed(domain, now)
        self.stats.record(False)
        log.warning("DNS lookup failed for %s", domain)
        return False

    async def _resolve_all(self, domain: str) -> Dict[str, List[object]]:
        tasks = {
            field_name: asyncio.create_task(self._resolve_record(domain, record_type))
            for record_type, field_name in DNS_RECORD_TYPES
        }

        results: Dict[str, List[object]] = {}
        for field_name, task in tasks.items():
            try:
                values = await task
            except Exception as exc:  # pragma: no cover - defensive
                log.debug("%s lookup crashed for %s: %s", field_name, domain, exc)
                continue

            if values:
                results[field_name] = values

        return results

    async def _resolve_record(self, domain: str, record_type: str) -> List[object]:
        timeout = self.settings.dns_timeout

        def _lookup() -> List[object]:
            result: List[object] = []
            res = resolver.Resolver()
            res.timeout = timeout
            res.lifetime = timeout
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
                    text = item.to_text().replace("\\", "").lower()
                    if result:
                        result[0] = text
                    else:
                        result.append(text)
                elif record_type == "CNAME":
                    result.append({"target": item.target.to_unicode().strip(".").lower()})
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
        update_doc = {
            "$set": {"updated": timestamp},
            "$setOnInsert": {"created": timestamp, "domain": domain},
            "$unset": {"claimed": "", "dns_lookup_failed": ""},
        }

        add_to_set = {
            field: {"$each": values}
            for field, values in records.items()
            if values
        }
        if add_to_set:
            update_doc["$addToSet"] = add_to_set

        await self.mongo.db.dns.update_one({"domain": domain}, update_doc, upsert=True)

    async def _mark_failed(self, domain: str, timestamp: datetime) -> None:
        await self.mongo.db.dns.update_one(
            {"domain": domain},
            {
                "$set": {"updated": timestamp, "dns_lookup_failed": timestamp},
                "$setOnInsert": {"created": timestamp, "domain": domain},
                "$unset": {"claimed": ""},
            },
            upsert=True,
        )

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
            log.exception("Unhandled error while resolving %s: %s", job.domain, exc)
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
            message = Message(domain.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT)
            await exchange.publish(message, routing_key=queue_name)
            sent += 1
            if sent and sent % 1000 == 0:
                log.info("Queued %d domains so far", sent)

        stop_message = Message(STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


async def iter_pending_domains(
    sync_client: MongoClient,
    batch_size: int = 10_000,
) -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()

    def _iterator() -> List[str]:
        results: List[str] = []
        cursor = sync_client.ip_data.dns.find(
            {"updated": {"$exists": False}},
            {"domain": 1},
            sort=[("$natural", 1)],
            batch_size=batch_size,
        )
        try:
            for doc in cursor:
                domain = doc.get("domain")
                if domain:
                    results.append(domain)
        finally:
            cursor.close()
        return results

    domains = await loop.run_in_executor(None, _iterator)
    for domain in domains:
        yield domain


async def direct_worker(settings: DirectWorkerSettings) -> str:
    runtime = DNSRuntime(settings)
    try:
        cursor = runtime.mongo.db.dns.find(
            {"updated": {"$exists": False}},
            {"domain": 1},
            sort=[("$natural", 1)],
            skip=settings.skip,
            limit=settings.limit,
        )

        pending: Set[asyncio.Task[None]] = set()
        semaphore = asyncio.Semaphore(max(1, settings.concurrency))

        async for doc in cursor:
            domain = doc.get("domain")
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
@click.option("--host", "-h", type=str, required=True, help="MongoDB host")
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
    host: str,
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
        mongo_host=host,
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
            raise click.BadParameter("RabbitMQ URL is required when --service is set")

        worker_args = [WorkerSettings(**base_settings.__dict__) for _ in range(worker)]
        if worker == 1:
            click.echo(run_worker(worker_args[0]))
        else:
            with multiprocessing.Pool(processes=worker) as pool:
                log.info("Spawned %d worker processes (service mode)", worker)
                for result in pool.imap_unordered(run_worker, worker_args):
                    click.echo(result)
        return

    sync_client = MongoClient(f"mongodb://{host}:27017", tz_aware=True)
    pending_filter = {"updated": {"$exists": False}}
    total_docs = sync_client.ip_data.dns.count_documents(pending_filter)
    click.echo(f"[INFO] total domains to resolve: {total_docs}")

    if total_docs == 0:
        sync_client.close()
        click.echo("[INFO] Nothing to resolve")
        return

    if rabbitmq_url:
        domains = iter_pending_domains(sync_client)
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
        log.info("Queued %d domains onto RabbitMQ queue '%s'", published, queue_name)
        sync_client.close()

        worker_args = [WorkerSettings(**base_settings.__dict__) for _ in range(worker)]
        with multiprocessing.Pool(processes=worker) as pool:
            log.info("Spawned %d worker processes", worker)
            for result in pool.imap_unordered(run_worker, worker_args):
                click.echo(result)
    else:
        sync_client.close()
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


if __name__ == "__main__":
    main()
