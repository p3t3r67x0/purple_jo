#!/usr/bin/env python3
"""Distributed HTTP header extractor backed by RabbitMQ and MongoDB."""

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
import httpx
from fake_useragent import UserAgent
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 400
DEFAULT_CONCURRENCY = 200
DEFAULT_TIMEOUT = 5.0
DEFAULT_SCHEMES: Tuple[str, ...] = ("http", "https")


log = logging.getLogger("extract_header")


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


def build_user_agent() -> str:
    try:
        return UserAgent().random
    except Exception as exc:  # pragma: no cover - best effort
        log.debug("Falling back to static user agent: %s", exc)
        return "Mozilla/5.0 (compatible; purple_jo-header/1.0)"


@dataclass
class WorkerSettings:
    mongo_host: str
    rabbitmq_url: Optional[str]
    queue_name: str
    prefetch: int
    concurrency: int
    request_timeout: float
    log_level: int
    schemes: Tuple[str, ...] = DEFAULT_SCHEMES
    verbose_headers: bool = False


@dataclass
class DirectWorkerSettings(WorkerSettings):
    skip: int = 0
    limit: int = 0


@dataclass
class HeaderJob:
    domain: str
    message: aio_pika.IncomingMessage
    stop: bool = False


@dataclass
class HeaderStats:
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


class HeaderRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.mongo = MongoAsync(settings.mongo_host)
        self.stats = HeaderStats()
        timeout = httpx.Timeout(settings.request_timeout)
        limits = httpx.Limits(max_keepalive_connections=settings.concurrency * 2)
        self.http = httpx.AsyncClient(
            http2=True,
            timeout=timeout,
            limits=limits,
            follow_redirects=True,
        )
        self.user_agent = build_user_agent()

    async def close(self) -> None:
        await self.http.aclose()
        self.mongo.close()

    async def process_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain:
            return False

        try:
            headers, final_url, chain = await self._fetch_headers(domain)
        except Exception as exc:
            log.debug("Header fetch failed for %s: %s", domain, exc)
            await self._mark_failed(domain)
            self.stats.record(False)
            log.warning("Header scan failed for %s", domain)
            return False

        await self._store_headers(domain, headers, final_url, chain)
        self.stats.record(True)
        self._log_headers(domain, headers, final_url, chain)
        return True

    async def _fetch_headers(self, domain: str) -> Tuple[Dict[str, str], str, List[str]]:
        last_error: Optional[Exception] = None
        for scheme in self.settings.schemes:
            url = f"{scheme}://{domain}"
            try:
                response = await self.http.head(
                    url,
                    headers={"User-Agent": self.user_agent},
                    allow_redirects=True,
                )
                header_map = {k.lower(): v for k, v in response.headers.items()}
                header_map.update(
                    {
                        "status": str(response.status_code),
                        "version": str(response.http_version),
                    }
                )
                final_url = str(response.url)
                chain = [str(item.url) for item in response.history] + [final_url] if response.history else [final_url]
                log.debug("Fetched %s -> %s (%s)", domain, final_url, response.status_code)
                return header_map, final_url, chain
            except httpx.HTTPError as exc:
                last_error = exc
                continue
        raise last_error or RuntimeError("Unknown HTTP error")

    async def _store_headers(
        self,
        domain: str,
        headers: Dict[str, str],
        final_url: str,
        chain: List[str],
    ) -> None:
        timestamp = datetime.now()
        update_fields = {
            "header": headers,
            "header_final_url": final_url,
            "header_redirect_chain": chain,
            "updated": timestamp,
        }
        update_doc = {
            "$set": update_fields,
            "$setOnInsert": {"created": timestamp, "domain": domain},
            "$unset": {"header_scan_failed": ""},
        }

        await self.mongo.db.dns.update_one({"domain": domain}, update_doc, upsert=True)

    async def _mark_failed(self, domain: str) -> None:
        timestamp = datetime.now()
        await self.mongo.db.dns.update_one(
            {"domain": domain},
            {
                "$set": {"header_scan_failed": timestamp},
                "$setOnInsert": {"created": timestamp, "domain": domain},
            },
            upsert=True,
        )

    def _log_headers(
        self,
        domain: str,
        headers: Dict[str, str],
        final_url: str,
        chain: List[str],
    ) -> None:
        log.info("Header scan success %s -> %s", domain, final_url)
        if self.settings.verbose_headers:
            log.info("Headers for %s: %s", domain, headers)
            if len(chain) > 1:
                log.info("Redirect chain for %s: %s", domain, chain)


class HeaderConsumer:
    def __init__(self, settings: WorkerSettings, runtime: HeaderRuntime) -> None:
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

    async def _handle_job(self, job: HeaderJob) -> None:
        try:
            async with self._semaphore:
                await self.runtime.process_domain(job.domain)
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("Unhandled error while processing %s: %s", job.domain, exc)
        finally:
            with contextlib.suppress(Exception):
                await job.message.ack()


async def get_domains(settings: WorkerSettings) -> AsyncIterator[HeaderJob]:
    if not settings.rabbitmq_url:
        raise ValueError("RabbitMQ URL is required to consume header jobs")

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max(1, settings.prefetch))
        queue = await channel.declare_queue(settings.queue_name, durable=True)

        async with queue.iterator() as iterator:
            async for message in iterator:
                if message.body == STOP_SENTINEL:
                    yield HeaderJob(domain="", message=message, stop=True)
                    break

                domain = message.body.decode().strip()
                if not domain:
                    await message.ack()
                    continue

                yield HeaderJob(domain=domain, message=message)
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
            {
                "header": {"$exists": False},
                "header_scan_failed": {"$exists": False},
                "ports.port": {"$in": [80, 443]},
            },
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
    runtime = HeaderRuntime(settings)
    try:
        cursor = runtime.mongo.db.dns.find(
            {
                "header": {"$exists": False},
                "header_scan_failed": {"$exists": False},
                "ports.port": {"$in": [80, 443]},
            },
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
    runtime = HeaderRuntime(settings)
    try:
        consumer = HeaderConsumer(settings, runtime)
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
              help="Concurrent requests per worker")
@click.option("--request-timeout", type=float, default=DEFAULT_TIMEOUT, show_default=True,
              help="HTTP request timeout in seconds")
@click.option("--rabbitmq-url", "-r", type=str, default=None,
              help="RabbitMQ URL (enables distributed mode)")
@click.option("--queue-name", "-q", type=str, default="header_scans", show_default=True,
              help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
              help="RabbitMQ prefetch per worker")
@click.option("--purge-queue/--no-purge-queue", default=True,
              help="Purge the queue before enqueuing new domains")
@click.option("--service", is_flag=True,
              help="Run as a RabbitMQ-consuming header extraction service")
@click.option("--log-headers", is_flag=True,
              help="Log full headers after each successful fetch")
@click.option("--verbose", is_flag=True, help="Enable verbose debug logging")
def main(
    worker: int,
    host: str,
    concurrency: int,
    request_timeout: float,
    rabbitmq_url: Optional[str],
    queue_name: str,
    prefetch: int,
    purge_queue: bool,
    service: bool,
    log_headers: bool,
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
        request_timeout=request_timeout,
        log_level=log_level,
        verbose_headers=log_headers,
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
    pending_filter = {
        "header": {"$exists": False},
        "header_scan_failed": {"$exists": False},
        "ports.port": {"$in": [80, 443]},
    }
    total_docs = sync_client.ip_data.dns.count_documents(pending_filter)
    click.echo(f"[INFO] total domains to scan: {total_docs}")

    if total_docs == 0:
        sync_client.close()
        click.echo("[INFO] Nothing to scan")
        return

    if rabbitmq_url:
        domains = iter_pending_domains(sync_client)
        log.info("Publishing header jobs to RabbitMQ at %s", rabbitmq_url)
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
        click.echo(
            "[INFO] Published header jobs to RabbitMQ. Start workers with --service"
        )
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
