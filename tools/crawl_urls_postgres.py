#!/usr/bin/env python3
"""
Enhanced URL Crawler using PostgreSQL instead of MongoDB.

Migrated from crawl_urls.py to use SQLModel and PostgreSQL for data storage.
Maintains the same high-throughput architecture while using the new database backend.
"""

from __future__ import annotations

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
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, AsyncGenerator, Iterable, Set, Tuple
from urllib.parse import urljoin, urlparse

import aio_pika
import click
import httpx
import idna
from aio_pika import DeliveryMode, Message
from aio_pika.exceptions import AMQPConnectionError
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.postgres import CrawlStatus, Domain, Url


log = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

STOP_SENTINEL = b"__STOP__"


def utcnow() -> datetime:
    """Return current UTC datetime without timezone info."""
    return datetime.now(UTC).replace(tzinfo=None)


def build_user_agent() -> str:
    """Build a user agent string."""
    return "Mozilla/5.0 (X11; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0"


def configure_logging(level: str) -> None:
    """Configure logging for the crawler."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@dataclass
class CrawlStats:
    """Track crawl statistics."""
    processed: int = 0
    successful: int = 0
    failed: int = 0
    urls_found: int = 0
    urls_inserted: int = 0

    def record(self, success: bool, urls_inserted: int = 0) -> None:
        """Record a crawl result."""
        self.processed += 1
        if success:
            self.successful += 1
            self.urls_inserted += urls_inserted
        else:
            self.failed += 1

    def __str__(self) -> str:
        return (
            f"processed={self.processed}, success={self.successful}, "
            f"failed={self.failed}, urls_inserted={self.urls_inserted}"
        )


@dataclass
class WorkerSettings:
    """Settings for a rabbit worker."""
    rabbitmq_url: str
    queue_name: str
    postgres_dsn: str
    concurrency: int
    prefetch: int
    request_timeout: float
    connect_timeout: float
    max_redirects: int
    max_retries: int
    schemes: list[str]
    verbose_urls: bool
    log_level: str


@dataclass
class DirectWorkerSettings:
    """Settings for a direct worker."""
    postgres_dsn: str
    skip: int
    limit: int
    concurrency: int
    request_timeout: float
    connect_timeout: float
    max_redirects: int
    max_retries: int
    schemes: list[str]
    verbose_urls: bool
    log_level: str


@dataclass
class CrawlJob:
    """A crawl job from the queue."""
    domain: str
    message: aio_pika.abc.AbstractIncomingMessage
    stop: bool = False


class PostgresAsync:
    """Async PostgreSQL database operations for the crawler."""

    def __init__(self, postgres_dsn: str) -> None:
        # Convert asyncpg DSN to psycopg DSN if needed
        if postgres_dsn.startswith("postgresql+asyncpg://"):
            postgres_dsn = postgres_dsn.replace(
                "postgresql+asyncpg://", "postgresql+psycopg://"
            )
        elif postgres_dsn.startswith("postgresql://"):
            postgres_dsn = postgres_dsn.replace(
                "postgresql://", "postgresql+psycopg://"
            )
        elif not postgres_dsn.startswith("postgresql+psycopg://"):
            # If no scheme, assume we need psycopg
            if "://" not in postgres_dsn:
                postgres_dsn = f"postgresql+psycopg://{postgres_dsn}"

        self.postgres_dsn = postgres_dsn
        self._engine = None
        self._session_factory = None

    async def _ensure_engine(self) -> None:
        """Ensure the engine is initialized."""
        if self._engine is None:
            from sqlalchemy.ext.asyncio import (
                create_async_engine, async_sessionmaker
            )
            self._engine = create_async_engine(
                self.postgres_dsn,
                echo=False,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=3600,
                pool_pre_ping=True,
            )
            self._session_factory = async_sessionmaker(
                bind=self._engine,
                expire_on_commit=False,
                class_=AsyncSession,
            )

    async def get_session(self) -> AsyncSession:
        """Get a database session."""
        await self._ensure_engine()
        return self._session_factory()

    async def close(self) -> None:
        """Close the database connection."""
        if self._engine:
            await self._engine.dispose()


class CrawlRuntime:
    def __init__(self, settings: WorkerSettings | DirectWorkerSettings) -> None:
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
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
        await self.postgres.close()

    async def crawl_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain:
            return False

        # First, try to claim the domain to prevent duplicate crawling
        if not await self._claim_domain(domain):
            log.debug("Domain %s already claimed or being processed", domain)
            return False

        links: Set[str] = set()
        final_url = ""
        try:
            punycode = idna.encode(domain).decode("ascii")
        except idna.IDNAError:
            log.info("Crawl %s: invalid domain", domain)
            await self._mark_domain(domain, failed=True)
            self._log_crawl_result(
                domain, success=False, links_found=0, inserted=0, final_url=final_url)
            self.stats.record(False)
            return False

        attempt = 0
        while True:
            try:
                links, final_url = await self._fetch_links(punycode)
                break
            except Exception as exc:
                if attempt >= self.settings.max_retries:
                    log.info(
                        "Crawl %s: fetch failed after retries (%s)", domain, exc)
                    await self._mark_domain(domain, failed=True)
                    self._log_crawl_result(
                        domain, success=False, links_found=0, inserted=0, final_url=final_url)
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
                    log.debug("Redirected %s -> %s", base_url, final_url)

                content_type = response.headers.get("content-type", "").lower()
                if not content_type.startswith("text/html"):
                    log.debug("Skipping non-HTML content: %s", content_type)
                    return set(), final_url

                urls = self._extract_urls(response.text, base_url)
                log.debug("Found %d URLs on %s", len(urls), base_url)
                return urls, final_url

            except Exception as exc:
                errors.append(f"{scheme}: {exc}")
                continue

        # If we get here, all schemes failed
        raise Exception(f"All schemes failed: {'; '.join(errors)}")

    def _extract_urls(self, html_content: str, base_url: str) -> Set[str]:
        """Extract URLs from HTML content."""
        urls: Set[str] = set()
        try:
            soup = BeautifulSoup(html_content, "html.parser")
        except Exception as exc:
            log.debug("BeautifulSoup parsing failed: %s", exc)
            return urls

        for tag in soup.find_all(["a", "link"], href=True):
            href = tag.get("href", "").strip()
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
        """Store URLs in the PostgreSQL database."""
        if not urls:
            return 0

        async with await self.postgres.get_session() as session:
            inserted_count = 0
            url_objs = []
            for url in urls:
                url_obj = Url(url=url, created_at=utcnow())
                url_objs.append(url_obj)
            session.add_all(url_objs)
            try:
                await session.commit()
                inserted_count = len(url_objs)
            except IntegrityError:
                # Some URLs already exist (unique constraint violation)
                await session.rollback()
                # Try inserting one by one to count only new URLs
                for url_obj in url_objs:
                    session.add(url_obj)
                    try:
                        await session.commit()
                        inserted_count += 1
                    except IntegrityError:
                        await session.rollback()
                        continue
                    except Exception as exc:
                        log.warning("Failed to insert URL %s: %s",
                                    url_obj.url, exc)
                        await session.rollback()
                        continue
            except Exception as exc:
                log.warning("Failed to batch insert URLs: %s", exc)
                await session.rollback()
            return inserted_count

    async def _claim_domain(self, domain: str) -> bool:
        """Claim a domain for crawling to prevent duplicate work."""
        async with await self.postgres.get_session() as session:
            try:
                # Try to claim the domain atomically
                stmt = text("""
                    INSERT INTO crawl_status (domain_name, created_at, updated_at)
                    VALUES (:domain, :now, :now)
                    ON CONFLICT (domain_name) DO NOTHING
                    RETURNING id
                """)
                result = await session.execute(stmt, {
                    "domain": domain,
                    "now": utcnow()
                })
                claimed = result.fetchone() is not None
                await session.commit()
                return claimed
            except Exception as exc:
                log.warning("Failed to claim domain %s: %s", domain, exc)
                await session.rollback()
                return False

    async def _mark_domain(self, domain: str, failed: bool) -> None:
        """Mark a domain as crawled or failed in the crawl_status table."""
        async with await self.postgres.get_session() as session:
            try:
                # Find existing crawl status record
                stmt = select(CrawlStatus).where(
                    CrawlStatus.domain_name == domain)
                result = await session.exec(stmt)
                crawl_status = result.first()

                if crawl_status is None:
                    # This shouldn't happen if we claimed properly, but create if missing
                    crawl_status = CrawlStatus(
                        domain_name=domain,
                        created_at=utcnow(),
                        updated_at=utcnow(),
                    )
                    session.add(crawl_status)

                # Update status
                now = utcnow()
                crawl_status.updated_at = now
                if failed:
                    crawl_status.crawl_failed = now
                else:
                    crawl_status.domain_crawled = now

                await session.commit()

            except Exception as exc:
                log.warning("Failed to update domain %s: %s", domain, exc)
                await session.rollback()

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
                break

            task = asyncio.create_task(self._handle_job(job))
            self._pending.add(task)
            task.add_done_callback(self._pending.discard)

        await self._wait_for_pending()

    async def _handle_job(self, job: CrawlJob) -> None:
        async with self._semaphore:
            try:
                await self.runtime.crawl_domain(job.domain)
                await job.message.ack()
            except Exception as exc:
                log.exception("Job processing failed for %s: %s",
                              job.domain, exc)
                await job.message.reject(requeue=False)

    async def _wait_for_pending(self) -> None:
        if self._pending:
            log.info("Waiting for %d pending jobs to complete",
                     len(self._pending))
            await asyncio.gather(*self._pending, return_exceptions=True)


async def get_urls(settings: WorkerSettings) -> AsyncGenerator[CrawlJob, None]:
    """Get URLs from RabbitMQ queue."""
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=settings.prefetch)
        queue = await channel.declare_queue(settings.queue_name, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                if message.body == STOP_SENTINEL:
                    yield CrawlJob("", message, stop=True)
                    break

                domain = message.body.decode("utf-8")
                yield CrawlJob(domain, message)


async def rabbit_worker(settings: WorkerSettings) -> str:
    """Run a RabbitMQ-based worker."""
    runtime = CrawlRuntime(settings)
    try:
        consumer = RabbitConsumer(settings, runtime)
        await consumer.consume()
        return str(runtime.stats)
    finally:
        await runtime.close()


async def direct_worker(settings: DirectWorkerSettings) -> str:
    """Run a direct worker that processes domains from database."""
    runtime = CrawlRuntime(settings)
    try:
        async with await runtime.postgres.get_session() as session:
            # Get domains that haven't been crawled yet (no crawl_status record means not claimed)
            from sqlalchemy.orm import aliased

            # Use SQLModel approach instead of raw SQL
            cs = aliased(CrawlStatus)
            stmt = (
                select(Domain.name)
                .outerjoin(cs, Domain.name == cs.domain_name)
                .where(cs.id.is_(None))
                .order_by(Domain.id)
                .offset(settings.skip)
                .limit(settings.limit)
            )
            result = await session.exec(stmt)
            domains = result.all()

        if not domains:
            log.info("No domains to process")
            return str(runtime.stats)

        log.info("Processing %d domains", len(domains))

        # Process domains with semaphore for concurrency control
        semaphore = asyncio.Semaphore(settings.concurrency)

        async def process_domain(domain: str) -> None:
            async with semaphore:
                await runtime.crawl_domain(domain)

        tasks = [process_domain(domain) for domain in domains]
        await asyncio.gather(*tasks, return_exceptions=True)

        return str(runtime.stats)
    finally:
        await runtime.close()


def run_worker_rabbit(settings: WorkerSettings) -> str:
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
            message = Message(domain.encode("utf-8"),
                              delivery_mode=DeliveryMode.PERSISTENT)
            await exchange.publish(message, routing_key=queue_name)
            sent += 1
            if sent % 1000 == 0:
                log.info("Queued %d domains so far", sent)

        stop_message = Message(
            STOP_SENTINEL, delivery_mode=DeliveryMode.PERSISTENT)
        for _ in range(worker_count):
            await exchange.publish(stop_message, routing_key=queue_name)
    return sent


async def iter_pending_domains_postgres(postgres_dsn: str, batch_size: int = 10_000) -> AsyncGenerator[str, None]:
    """Iterate over domains that haven't been crawled yet."""
    postgres = PostgresAsync(postgres_dsn)
    try:
        async with await postgres.get_session() as session:
            offset = 0
            while True:
                from sqlalchemy.orm import aliased

                # Use SQLModel approach instead of raw SQL
                cs = aliased(CrawlStatus)
                stmt = (
                    select(Domain.name)
                    .outerjoin(cs, Domain.name == cs.domain_name)
                    .where(cs.id.is_(None))
                    .order_by(Domain.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                result = await session.exec(stmt)
                domains = result.all()

                if not domains:
                    break

                for domain in domains:
                    yield domain

                offset += batch_size
    finally:
        await postgres.close()


@click.command()
@click.option("--worker", "-w", type=int, default=4, show_default=True, help="Worker processes")
@click.option("--postgres-dsn", type=str, required=True, help="PostgreSQL DSN")
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
    "--max-retries", type=int, default=2, show_default=True,
    help="Maximum retries for failed requests"
)
@click.option(
    "--schemes", multiple=True, default=["https", "http"], show_default=True,
    help="URL schemes to try (in order)"
)
@click.option(
    "--verbose-urls", is_flag=True, help="Log each discovered URL"
)
@click.option(
    "--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="INFO", show_default=True
)
# RabbitMQ mode options
@click.option(
    "--rabbitmq-url", type=str, help="RabbitMQ URL (enables RabbitMQ mode)"
)
@click.option(
    "--queue-name", type=str, default="crawl_domains", show_default=True,
    help="RabbitMQ queue name"
)
@click.option(
    "--prefetch", type=int, default=10, show_default=True,
    help="RabbitMQ prefetch count per worker"
)
@click.option(
    "--enqueue", is_flag=True,
    help="Enqueue pending domains and exit (requires --rabbitmq-url)"
)
@click.option(
    "--purge-queue", is_flag=True,
    help="Purge the queue before enqueueing"
)
# Direct mode options
@click.option(
    "--skip", type=int, default=0, show_default=True,
    help="Skip this many domains (direct mode)"
)
@click.option(
    "--limit", type=int, default=10000, show_default=True,
    help="Process at most this many domains (direct mode)"
)
def main(
    worker: int,
    postgres_dsn: str,
    concurrency: int,
    request_timeout: float,
    connect_timeout: float,
    max_redirects: int,
    max_retries: int,
    schemes: tuple[str, ...],
    verbose_urls: bool,
    log_level: str,
    rabbitmq_url: str | None,
    queue_name: str,
    prefetch: int,
    enqueue: bool,
    purge_queue: bool,
    skip: int,
    limit: int,
) -> None:
    """URL crawler using PostgreSQL backend."""
    configure_logging(log_level)

    if enqueue:
        if not rabbitmq_url:
            click.echo(
                "--rabbitmq-url is required for --enqueue mode", err=True)
            sys.exit(1)

        async def enqueue_all():
            domains = []
            async for domain in iter_pending_domains_postgres(postgres_dsn):
                domains.append(domain)

            if not domains:
                log.info("No pending domains to enqueue")
                return

            log.info("Enqueueing %d domains", len(domains))
            sent = await enqueue_domains(rabbitmq_url, queue_name, domains, worker, purge_queue)
            log.info("Enqueued %d domains", sent)

        asyncio.run(enqueue_all())
        return

    if rabbitmq_url:
        # RabbitMQ mode
        settings = WorkerSettings(
            rabbitmq_url=rabbitmq_url,
            queue_name=queue_name,
            postgres_dsn=postgres_dsn,
            concurrency=concurrency,
            prefetch=prefetch,
            request_timeout=request_timeout,
            connect_timeout=connect_timeout,
            max_redirects=max_redirects,
            max_retries=max_retries,
            schemes=list(schemes),
            verbose_urls=verbose_urls,
            log_level=log_level,
        )

        with multiprocessing.Pool(worker) as pool:
            results = pool.map(run_worker_rabbit, [settings] * worker)

        success_count = sum(1 for r in results if not r.startswith("[ERROR]"))
        log.info("Workers completed: %d successful, %d failed",
                 success_count, worker - success_count)

        if success_count < worker:
            sys.exit(1)
    else:
        # Direct mode
        chunk_size = limit // worker
        remainder = limit % worker

        settings_list = []
        current_skip = skip
        for i in range(worker):
            worker_limit = chunk_size + (1 if i < remainder else 0)
            settings_list.append(DirectWorkerSettings(
                postgres_dsn=postgres_dsn,
                skip=current_skip,
                limit=worker_limit,
                concurrency=concurrency,
                request_timeout=request_timeout,
                connect_timeout=connect_timeout,
                max_redirects=max_redirects,
                max_retries=max_retries,
                schemes=list(schemes),
                verbose_urls=verbose_urls,
                log_level=log_level,
            ))
            current_skip += worker_limit

        with multiprocessing.Pool(worker) as pool:
            results = pool.map(run_worker_direct, settings_list)

        success_count = sum(1 for r in results if not r.startswith("[ERROR]"))
        log.info("Workers completed: %d successful, %d failed",
                 success_count, worker - success_count)

        if success_count < worker:
            sys.exit(1)


if __name__ == "__main__":
    main()
