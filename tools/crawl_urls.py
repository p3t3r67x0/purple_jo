#!/usr/bin/env python3
"""High-throughput domain crawler backed by MongoDB and RabbitMQ.

The crawler is designed for sustained throughput above 100k domains/hour.
It keeps a small, shared HTTP connection pool per worker process, performs
IDNA-safe fetches, and streams crawl jobs through RabbitMQ so workers can
scale horizontally across machines.
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
from typing import AsyncIterator, Iterable, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

try:  # optional but valuable for large-scale workloads
    import uvloop  # type: ignore

    uvloop.install()
except Exception:  # pragma: no cover - uvloop is optional
    pass

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
import httpx
import idna
from fake_useragent import UserAgent
from lxml import html
from lxml.etree import ParserError, XMLSyntaxError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    CursorNotFound,
    DuplicateKeyError,
    WriteError,
)


STOP_SENTINEL = b"__STOP__"
DEFAULT_SCHEMES: Tuple[str, ...] = ("https", "http")
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


log = logging.getLogger("crawler")


def build_user_agent() -> str:
    try:
        return UserAgent().chrome
    except Exception as exc:  # pragma: no cover - best effort
        log.debug("Falling back to static user agent: %s", exc)
        return "Mozilla/5.0 (compatible; purple_jo/1.0)"


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
    concurrency: int
    rabbitmq_url: Optional[str]
    queue_name: str
    prefetch: int
    log_level: int
    request_timeout: float
    connect_timeout: float
    max_redirects: int
    max_retries: int
    schemes: Tuple[str, ...] = DEFAULT_SCHEMES
    verbose_urls: bool = False


@dataclass
class DirectWorkerSettings(WorkerSettings):
    skip: int = 0
    limit: int = 0


@dataclass
class CrawlJob:
    domain: str
    message: aio_pika.IncomingMessage
    stop: bool = False


@dataclass
class CrawlStats:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    urls_inserted: int = 0

    def record(self, success: bool, inserted: int = 0) -> None:
        self.processed += 1
        if success:
            self.succeeded += 1
        else:
            self.failed += 1
        self.urls_inserted += inserted

    def summary(self) -> str:
        return (
            f"processed={self.processed} succeeded={self.succeeded} "
            f"failed={self.failed} urls={self.urls_inserted}"
        )


class MongoAsync:
    def __init__(self, host: str) -> None:
        self._client = AsyncIOMotorClient(f"mongodb://{host}:27017", tz_aware=True)
        self.url_data = self._client.url_data
        self.ip_data = self._client.ip_data

    def close(self) -> None:
        self._client.close()


class CrawlRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.mongo = MongoAsync(settings.mongo_host)
        limits = httpx.Limits(
            max_connections=max(settings.concurrency * 4, 200),
            max_keepalive_connections=max(settings.concurrency * 2, 100),
        )
        timeout = httpx.Timeout(
            settings.request_timeout,
            connect=settings.connect_timeout,
        )
        headers = dict(DEFAULT_HEADERS)
        headers["User-Agent"] = build_user_agent()
        self.http = httpx.AsyncClient(
            http2=True,
            timeout=timeout,
            limits=limits,
            max_redirects=settings.max_redirects,
            headers=headers,
            verify=True,
            follow_redirects=True,
        )
        self.stats = CrawlStats()

    async def close(self) -> None:
        await self.http.aclose()
        self.mongo.close()

    async def crawl_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain:
            return False

        links: Set[str] = set()
        final_url = ""
        try:
            punycode = idna.encode(domain).decode("ascii")
        except idna.IDNAError:
            log.info("Crawl %s: invalid domain", domain)
            await self._mark_domain(domain, failed=True)
            self._log_crawl_result(domain, success=False, links_found=0, inserted=0, final_url=final_url)
            self.stats.record(False)
            return False

        attempt = 0
        while True:
            try:
                links, final_url = await self._fetch_links(punycode)
                break
            except Exception as exc:
                if attempt >= self.settings.max_retries:
                    log.info("Crawl %s: fetch failed after retries (%s)", domain, exc)
                    await self._mark_domain(domain, failed=True)
                    self._log_crawl_result(domain, success=False, links_found=0, inserted=0, final_url=final_url)
                    self.stats.record(False)
                    return False
                backoff = min(1.0 * (attempt + 1), 5.0)
                await asyncio.sleep(backoff)
                attempt += 1

        inserted = 0
        if links:
            inserted = await self._store_links(links)
            if self.settings.verbose_urls:
                for link in sorted(links):
                    log.info("Discovered %s -> %s", domain, link)
        self._log_crawl_result(
            domain,
            success=True,
            links_found=len(links),
            inserted=inserted,
            final_url=final_url or domain,
        )

        await self._mark_domain(domain, failed=False)
        self.stats.record(True, inserted)
        return True

    async def _fetch_links(self, punycode_domain: str) -> Tuple[Set[str], str]:
        errors = []
        for scheme in self.settings.schemes:
            base_url = f"{scheme}://{punycode_domain}"
            try:
                response = await self.http.get(base_url)
                response.raise_for_status()
                final_url = str(response.url)
                if response.history:
                    log.debug("Crawl %s redirected to %s", base_url, final_url)
                return self._extract_links(response.text, final_url), final_url
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                errors.append(exc)
                continue
        if errors:
            raise errors[-1]
        return set(), ""

    @staticmethod
    def _extract_links(content: str, base_url: str) -> Set[str]:
        try:
            document = html.document_fromstring(content)
        except (ValueError, ParserError, XMLSyntaxError):
            return set()

        urls: Set[str] = set()
        for href in document.xpath("//a/@href"):
            href = href.strip().lower()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:", "+")):
                continue
            if href.startswith(("/", "?", "..")):
                href = urljoin(base_url, href)
            try:
                parsed = urlparse(href)
            except ValueError:
                continue
            if parsed.scheme in {"http", "https"} and parsed.netloc:
                urls.add(href)
        return urls

    async def _store_links(self, urls: Set[str]) -> int:
        documents = [
            {"url": url, "created": datetime.now()}
            for url in urls
        ]
        if not documents:
            return 0

        attempts = self.settings.max_retries + 1
        for attempt in range(attempts):
            try:
                result = await self.mongo.url_data.url.insert_many(documents, ordered=False)
                return len(result.inserted_ids)
            except BulkWriteError as exc:
                write_errors = exc.details.get("writeErrors", []) if exc.details else []
                non_duplicates = [err for err in write_errors if err.get("code") != 11000]
                if non_duplicates:
                    log.warning("Bulk insert errors: %s", non_duplicates[:3])
                duplicates = len(write_errors) - len(non_duplicates)
                return max(len(documents) - duplicates, 0)
            except AutoReconnect as exc:
                if attempt + 1 >= attempts:
                    log.warning("Insert failed after retries: %s", exc)
                    break
                await asyncio.sleep(min(1.0 * (attempt + 1), 5.0))
            except (DuplicateKeyError, WriteError) as exc:
                log.debug("Insert skipped: %s", exc)
                return 0
        return 0

    async def _mark_domain(self, domain: str, failed: bool) -> None:
        field = "crawl_failed" if failed else "domain_crawled"
        try:
            await self.mongo.ip_data.dns.update_one(
                {"domain": domain},
                {"$set": {field: datetime.now()}},
                upsert=False,
            )
        except DuplicateKeyError:
            pass
        except WriteError as exc:
            log.warning("Failed to update domain %s: %s", domain, exc)


    def _log_crawl_result(
        self,
        domain: str,
        success: bool,
        links_found: int,
        inserted: int,
        final_url: str,
    ) -> None:
        status = "success" if success else "failed"
        log.info(
            "Crawl %s: %s (final_url=%s, links=%d, inserted=%d)",
            domain,
            status,
            final_url or domain,
            links_found,
            inserted,
        )


class RabbitConsumer:
    def __init__(self, settings: WorkerSettings, runtime: CrawlRuntime) -> None:
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

        async for job in get_urls(self.settings):
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

    async def _handle_job(self, job: CrawlJob) -> None:
        try:
            async with self._semaphore:
                await self.runtime.crawl_domain(job.domain)
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("Unhandled error while crawling %s: %s", job.domain, exc)
        finally:
            with contextlib.suppress(Exception):
                await job.message.ack()


async def get_urls(settings: WorkerSettings) -> AsyncIterator[CrawlJob]:
    if not settings.rabbitmq_url:
        raise ValueError("RabbitMQ URL is required to consume crawl jobs")

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max(1, settings.prefetch))
        queue = await channel.declare_queue(settings.queue_name, durable=True)

        async with queue.iterator() as iterator:
            async for message in iterator:
                if message.body == STOP_SENTINEL:
                    yield CrawlJob(domain="", message=message, stop=True)
                    break

                domain = message.body.decode().strip()
                if not domain:
                    await message.ack()
                    continue

                yield CrawlJob(domain=domain, message=message)
    finally:
        await connection.close()


async def direct_worker(settings: DirectWorkerSettings) -> str:
    runtime = CrawlRuntime(settings)
    try:
        semaphore = asyncio.Semaphore(max(1, settings.concurrency))
        cursor = runtime.mongo.ip_data.dns.find(
            {"domain_crawled": {"$exists": False}},
            {"domain": 1},
            sort=[("$natural", 1)],
            skip=settings.skip,
            limit=settings.limit,
        )

        pending: Set[asyncio.Task[None]] = set()
        try:
            async for doc in cursor:
                domain = doc.get("domain")
                if not domain:
                    continue

                task = asyncio.create_task(_process_direct_domain(runtime, semaphore, domain))
                pending.add(task)
                task.add_done_callback(pending.discard)
        except CursorNotFound:
            log.warning(
                "Cursor lost for slice %d:%d", settings.skip, settings.skip + settings.limit
            )

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return runtime.stats.summary()
    finally:
        await runtime.close()


async def _process_direct_domain(runtime: CrawlRuntime, semaphore: asyncio.Semaphore, domain: str) -> None:
    async with semaphore:
        await runtime.crawl_domain(domain)


async def rabbit_worker(settings: WorkerSettings) -> str:
    runtime = CrawlRuntime(settings)
    try:
        consumer = RabbitConsumer(settings, runtime)
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
        summary = asyncio.run(rabbit_worker(settings))
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


async def enqueue_domains(
    rabbitmq_url: str,
    queue_name: str,
    domains: Iterable[str],
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
        for domain in domains:
            domain = domain.strip()
            if not domain:
                continue
            message = Message(domain.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT)
            await exchange.publish(message, routing_key=queue_name)
            sent += 1
            if sent % 1000 == 0:
                log.info("Queued %d domains so far", sent)

        stop_message = Message(STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


def iter_pending_domains(sync_client: MongoClient, batch_size: int = 10_000) -> Iterable[str]:
    cursor = sync_client.ip_data.dns.find(
        {"domain_crawled": {"$exists": False}},
        {"domain": 1},
        sort=[("$natural", 1)],
        batch_size=batch_size,
    )
    try:
        for doc in cursor:
            domain = doc.get("domain")
            if domain:
                yield domain
    finally:
        cursor.close()


@click.command()
@click.option("--worker", "-w", type=int, default=4, show_default=True, help="Worker processes")
@click.option("--host", "-h", type=str, required=True, help="MongoDB host")
@click.option(
    "--concurrency", "-c", type=int, default=500, show_default=True,
    help="Concurrent domains per worker"
)
@click.option(
    "--request-timeout", type=float, default=10.0, show_default=True,
    help="Per-request timeout in seconds"
)
@click.option(
    "--connect-timeout", type=float, default=5.0, show_default=True,
    help="HTTP connect timeout in seconds"
)
@click.option(
    "--max-redirects", type=int, default=5, show_default=True,
    help="Maximum redirects followed per request"
)
@click.option(
    "--max-retries", type=int, default=1, show_default=True,
    help="Automatic retries for transient insert errors"
)
@click.option(
    "--rabbitmq-url", "-r", type=str, default=None,
    help="RabbitMQ URL (enables distributed mode)"
)
@click.option(
    "--queue-name", "-q", type=str, default="crawl_domains", show_default=True,
    help="RabbitMQ queue name"
)
@click.option(
    "--prefetch", type=int, default=400, show_default=True,
    help="RabbitMQ prefetch per worker"
)
@click.option(
    "--purge-queue/--no-purge-queue", default=True,
    help="Purge the queue before enqueuing new domains"
)
@click.option(
    "--verbose", is_flag=True,
    help="Enable verbose debug logging"
)
@click.option(
    "--log-urls", is_flag=True,
    help="Log every discovered URL"
)
@click.option(
    "--service", is_flag=True,
    help="Run as a RabbitMQ-consuming crawl service"
)
def main(
    worker: int,
    host: str,
    concurrency: int,
    request_timeout: float,
    connect_timeout: float,
    max_redirects: int,
    max_retries: int,
    rabbitmq_url: Optional[str],
    queue_name: str,
    prefetch: int,
    purge_queue: bool,
    verbose: bool,
    log_urls: bool,
    service: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    worker = max(1, worker)
    concurrency = max(1, concurrency)
    prefetch = max(1, prefetch)

    base_settings = WorkerSettings(
        mongo_host=host,
        concurrency=concurrency,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        log_level=log_level,
        request_timeout=request_timeout,
        connect_timeout=connect_timeout,
        max_redirects=max_redirects,
        max_retries=max_retries,
        schemes=DEFAULT_SCHEMES,
        verbose_urls=log_urls,
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
    pending_filter = {"domain_crawled": {"$exists": False}}
    total_docs = sync_client.ip_data.dns.count_documents(pending_filter)
    click.echo(f"[INFO] total documents to process: {total_docs}")

    if total_docs == 0:
        sync_client.close()
        click.echo("[INFO] Nothing to crawl")
        return

    if rabbitmq_url:
        domains = iter_pending_domains(sync_client)
        log.info("Publishing crawl jobs to RabbitMQ at %s", rabbitmq_url)
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
