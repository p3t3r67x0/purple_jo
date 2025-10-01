#!/usr/bin/env python3
"""Distributed HTTP header extractor backed by RabbitMQ and PostgreSQL."""

from __future__ import annotations

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

import asyncio
import contextlib
import logging
import math
import multiprocessing
import os
from pathlib import Path
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
import httpx

from async_sqlmodel_helpers import resolve_async_dsn
from fake_useragent import UserAgent
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql import Select
from sqlmodel import Field, SQLModel

# Ensure the repository root is on ``sys.path`` so shared modules can be imported
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from shared.models.postgres import Domain, PortService


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 400
DEFAULT_CONCURRENCY = 200
DEFAULT_TIMEOUT = 5.0
DEFAULT_SCHEMES: Tuple[str, ...] = ("http", "https")
log = logging.getLogger("extract_header")


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with PostgreSQL columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


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
    postgres_dsn: str
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


class HeaderScanState(SQLModel, table=True):
    """Track domains whose header scans previously failed."""

    __tablename__ = "header_scan_state"

    domain_name: str = Field(primary_key=True, max_length=255)
    header_scan_failed: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=utcnow, nullable=False)


class PostgresAsync:
    def __init__(self, dsn: str) -> None:
        self._raw_dsn = dsn
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker[AsyncSession]] = None

    def _get_session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self._session_factory is None:
            resolved = resolve_async_dsn(self._raw_dsn)
            self._engine = create_async_engine(
                resolved,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600,
                future=True,
            )
            self._session_factory = async_sessionmaker(
                bind=self._engine,
                expire_on_commit=False,
                class_=AsyncSession,
            )
        return self._session_factory

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        session_factory = self._get_session_factory()
        async with session_factory() as session:
            yield session

    async def ensure_header_scan_state(self) -> None:
        if self._engine is None:
            self._get_session_factory()
        assert self._engine is not None
        async with self._engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: HeaderScanState.__table__.create(
                    sync_conn, checkfirst=True
                )
            )

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None


def _pending_domains_select() -> Select:
    return (
        select(Domain.name)
        .select_from(Domain)
        .join(
            HeaderScanState,
            HeaderScanState.domain_name == Domain.name,
            isouter=True,
        )
        .where(Domain.header_content_type.is_(None))
        .where(Domain.header_status.is_(None))
        .where(HeaderScanState.header_scan_failed.is_(None))
    )


class HeaderRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.db = PostgresAsync(settings.postgres_dsn)
        self.stats = HeaderStats()
        timeout = httpx.Timeout(settings.request_timeout)
        limits = httpx.Limits(
            max_keepalive_connections=settings.concurrency * 2
        )
        self.http = httpx.AsyncClient(
            http2=True,
            timeout=timeout,
            limits=limits,
            follow_redirects=True,
        )
        self.user_agent = build_user_agent()

    async def close(self) -> None:
        await self.http.aclose()
        await self.db.close()

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

    async def _fetch_headers(
        self, domain: str
    ) -> Tuple[Dict[str, str], str, List[str]]:
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
                if response.history:
                    chain = [str(item.url) for item in response.history]
                    chain.append(final_url)
                else:
                    chain = [final_url]
                log.debug(
                    "Fetched %s -> %s (%s)",
                    domain, final_url, response.status_code
                )
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
        timestamp = utcnow()
        # Extract specific headers to store in dedicated columns
        header_updates = {
            "header_content_type": headers.get("content-type"),
            "header_content_length": (
                int(headers.get("content-length"))
                if headers.get("content-length", "").isdigit()
                else None
            ),
            "header_content_encoding": headers.get("content-encoding"),
            "header_cache_control": headers.get("cache-control"),
            "header_etag": headers.get("etag"),
            "header_set_cookie": headers.get("set-cookie"),
            "header_location": headers.get("location"),
            "header_www_authenticate": headers.get("www-authenticate"),
            "header_access_control_allow_origin": headers.get(
                "access-control-allow-origin"
            ),
            "header_strict_transport_security": headers.get(
                "strict-transport-security"
            ),
            "header_status": headers.get("status"),
            "header_server": headers.get("server"),
            "header_x_powered_by": headers.get("x-powered-by"),
        }

        async with self.db.session() as session:
            update_values = {
                column: value for column, value in header_updates.items() if value is not None
            }
            statement = insert(Domain).values(
                name=domain,
                created_at=timestamp,
                updated_at=timestamp,
                **update_values,
            )
            if update_values:
                statement = statement.on_conflict_do_update(
                    index_elements=[Domain.__table__.c.name],
                    set_={**update_values, "updated_at": timestamp},
                )
            else:
                statement = statement.on_conflict_do_update(
                    index_elements=[Domain.__table__.c.name],
                    set_={"updated_at": timestamp},
                )

            await session.execute(statement)
            await session.commit()

    async def _mark_failed(self, domain: str) -> None:
        timestamp = utcnow()
        await self.db.ensure_header_scan_state()

        async with self.db.session() as session:
            result = await session.execute(
                select(HeaderScanState).where(
                    HeaderScanState.domain_name == domain
                )
            )
            state = result.scalar_one_or_none()

            if state is None:
                state = HeaderScanState(
                    domain_name=domain, header_scan_failed=timestamp
                )
                session.add(state)
            else:
                state.header_scan_failed = timestamp

            await session.commit()

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
    postgres_dsn: str,
    batch_size: int = 10_000,
) -> AsyncIterator[str]:
    db = PostgresAsync(postgres_dsn)
    try:
        await db.ensure_header_scan_state()
        async with db.session() as session:
            stmt = (
                _pending_domains_select()
                .order_by(Domain.updated_at.desc())
                .limit(batch_size)
            )
            result = await session.execute(stmt)
            for domain_name in result.scalars():
                yield domain_name
    finally:
        await db.close()


async def direct_worker(settings: DirectWorkerSettings) -> str:
    runtime = HeaderRuntime(settings)
    try:
        await runtime.db.ensure_header_scan_state()

        async with runtime.db.session() as session:
            stmt = (
                _pending_domains_select()
                .order_by(Domain.updated_at.desc())
                .offset(settings.skip)
                .limit(settings.limit)
            )
            rows = (await session.execute(stmt)).scalars().all()

        pending: Set[asyncio.Task[None]] = set()
        semaphore = asyncio.Semaphore(max(1, settings.concurrency))

        for domain in rows:
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


class HeaderTool:
    """Coordinate CLI interactions for header extraction."""

    @classmethod
    def run(
        cls,
        postgres_dsn: str,
        worker: int,
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
            postgres_dsn=postgres_dsn,
            rabbitmq_url=rabbitmq_url,
            queue_name=queue_name,
            prefetch=prefetch,
            concurrency=concurrency,
            request_timeout=request_timeout,
            log_level=log_level,
            verbose_headers=log_headers,
        )

        if service:
            cls._run_service_mode(worker, base_settings, rabbitmq_url)
            return

        asyncio.run(
            cls._count_and_process(
                postgres_dsn=postgres_dsn,
                worker=worker,
                base_settings=base_settings,
                rabbitmq_url=rabbitmq_url,
                queue_name=queue_name,
                purge_queue=purge_queue,
            )
        )

    @classmethod
    def _run_service_mode(
        cls,
        worker: int,
        base_settings: WorkerSettings,
        rabbitmq_url: Optional[str],
    ) -> None:
        if not rabbitmq_url:
            raise click.BadParameter(
                "RabbitMQ URL is required when --service is set",
            )

        worker_args = [
            WorkerSettings(**base_settings.__dict__) for _ in range(worker)
        ]
        if worker == 1:
            click.echo(run_worker(worker_args[0]))
            return

        with multiprocessing.Pool(processes=worker) as pool:
            log.info("Spawned %d worker processes (service mode)", worker)
            for result in pool.imap_unordered(run_worker, worker_args):
                click.echo(result)

    @classmethod
    async def _count_and_process(
        cls,
        postgres_dsn: str,
        worker: int,
        base_settings: WorkerSettings,
        rabbitmq_url: Optional[str],
        queue_name: str,
        purge_queue: bool,
    ) -> None:
        db = PostgresAsync(postgres_dsn)
        try:
            await db.ensure_header_scan_state()
            async with db.session() as session:
                count_stmt = select(func.count()).select_from(
                    _pending_domains_select().subquery()
                )
                total_docs = (await session.execute(count_stmt)).scalar_one()

            click.echo(f"[INFO] total domains to scan: {total_docs}")

            if total_docs == 0:
                click.echo("[INFO] Nothing to scan")
                return

            if rabbitmq_url:
                domains = iter_pending_domains(postgres_dsn)
                log.info(
                    "Publishing header jobs to RabbitMQ at %s",
                    rabbitmq_url,
                )
                published = await enqueue_domains(
                    rabbitmq_url=rabbitmq_url,
                    queue_name=queue_name,
                    domains=domains,
                    worker_count=worker,
                    purge_queue=purge_queue,
                )
                log.info(
                    "Queued %d domains onto RabbitMQ queue '%s'",
                    published,
                    queue_name,
                )
                click.echo(
                    "[INFO] Published header jobs to RabbitMQ. "
                    "Start workers with --service",
                )
                return

            chunk_size = math.ceil(total_docs / worker)
            tasks: List[DirectWorkerSettings] = []
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

            with multiprocessing.Pool(processes=worker) as process_pool:
                log.info("Spawned %d direct worker processes", worker)
                results = process_pool.imap_unordered(
                    run_worker_direct, tasks
                )
                for result in results:
                    click.echo(result)
        finally:
            await db.close()


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
        "Worker process %s starting (mode=direct, slice=%d:%d, "
        "concurrency=%d)",
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
@click.option(
    "--postgres-dsn", type=str, required=True, help="PostgreSQL DSN"
)
@click.option(
    "--worker", "-w", type=int, default=4, show_default=True,
    help="Worker processes"
)
@click.option(
    "--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
    show_default=True, help="Concurrent requests per worker"
)
@click.option(
    "--request-timeout", type=float, default=DEFAULT_TIMEOUT,
    show_default=True, help="HTTP request timeout in seconds"
)
@click.option(
    "--rabbitmq-url", "-r", type=str, default=None,
    help="RabbitMQ URL (enables distributed mode)"
)
@click.option(
    "--queue-name", "-q", type=str, default="header_scans",
    show_default=True, help="RabbitMQ queue name"
)
@click.option(
    "--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
    help="RabbitMQ prefetch per worker"
)
@click.option(
    "--purge-queue/--no-purge-queue", default=True,
    help="Purge the queue before enqueuing new domains"
)
@click.option(
    "--service", is_flag=True,
    help="Run as a RabbitMQ-consuming header extraction service"
)
@click.option(
    "--log-headers", is_flag=True,
    help="Log full headers after each successful fetch"
)
@click.option(
    "--verbose", is_flag=True, help="Enable verbose debug logging"
)
def main(
    postgres_dsn: str,
    worker: int,
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
    HeaderTool.run(
        postgres_dsn=postgres_dsn,
        worker=worker,
        concurrency=concurrency,
        request_timeout=request_timeout,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        purge_queue=purge_queue,
        service=service,
        log_headers=log_headers,
        verbose=verbose,
    )


if __name__ == "__main__":
    CLITool(main).run()
