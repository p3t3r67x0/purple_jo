#!/usr/bin/env python3
"""Distributed TLS certificate scanner backed by RabbitMQ and PostgreSQL."""

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
import math
import multiprocessing
import os
import re
import ssl
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.postgres import Domain, PortService, SSLData, SSLSubjectAltName


STOP_SENTINEL = b"__STOP__"
DEFAULT_PREFETCH = 400
DEFAULT_CONCURRENCY = 100
DEFAULT_TIMEOUT = 5.0
DEFAULT_PORTS: Tuple[int, ...] = (443,)
TLS_VERSION_MAP = {
    "TLSv1.0": ssl.TLSVersion.TLSv1,
    "TLSv1.1": ssl.TLSVersion.TLSv1_1,
    "TLSv1.2": ssl.TLSVersion.TLSv1_2,
    "TLSv1.3": ssl.TLSVersion.TLSv1_3,
}


log = logging.getLogger("ssl_cert_scanner")


ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_file(path: Path) -> dict:
    """Parse a .env file and return key-value pairs."""
    env_vars = {}
    if not path.exists():
        return env_vars

    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip().strip('"\'')
    return env_vars


def resolve_dsn() -> str:
    """Resolve PostgreSQL DSN from environment or .env file."""
    # First try environment variables
    if "POSTGRES_DSN" in os.environ:
        dsn = os.environ["POSTGRES_DSN"]
    else:
        # Try to load from .env file
        env_vars = _parse_env_file(ENV_PATH)
        dsn = env_vars.get("POSTGRES_DSN")

        if not dsn:
            raise ValueError(
                "POSTGRES_DSN not found in environment or .env file"
            )

    # Return the DSN as-is since we're using SQLModel/SQLAlchemy
    return dsn


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
    postgres_dsn: str
    rabbitmq_url: Optional[str]
    queue_name: str
    prefetch: int
    concurrency: int
    timeout: float
    ports: Tuple[int, ...]
    log_level: int
    verbose_tls: bool = False


@dataclass
class DirectWorkerSettings(WorkerSettings):
    skip: int = 0
    limit: int = 0


@dataclass
class SSLJob:
    domain: str
    message: aio_pika.IncomingMessage
    stop: bool = False


@dataclass
class ScanStats:
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


class PostgresAsync:
    def __init__(self, postgres_dsn: str) -> None:
        self.postgres_dsn = postgres_dsn
        self.engine = create_async_engine(postgres_dsn)
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def get_session(self) -> AsyncSession:
        """Get a database session."""
        return self.session_factory()

    async def close(self) -> None:
        """Close database connections."""
        try:
            await self.engine.dispose()
        except Exception:
            pass


class SSLRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
        self.stats = ScanStats()

    async def close(self) -> None:
        await self.postgres.close()

    async def process_domain(self, domain: str) -> bool:
        domain = domain.strip().lower()
        if not domain:
            return False

        cert_data = await self._extract_certificate(domain)
        timestamp = datetime.now()

        if cert_data:
            await self._store_success(domain, cert_data, timestamp)
            self.stats.record(True)
            self._log_success(domain, cert_data)
            return True

        await self._mark_failed(domain, timestamp)
        self.stats.record(False)
        log.warning("TLS scan failed for %s", domain)
        return False

    async def _extract_certificate(self, domain: str) -> Optional[Dict[str, object]]:
        for port in self.settings.ports:
            result = await self._scan_port(domain, port)
            if result:
                return result
        return None

    async def _scan_port(self, domain: str, port: int) -> Optional[Dict[str, object]]:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_OPTIONAL  # Get cert info

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    domain,
                    port,
                    ssl=ssl_ctx,
                    server_hostname=domain,
                ),
                timeout=self.settings.timeout,
            )
        except Exception as exc:
            log.debug("Handshake failed for %s:%d (%s)", domain, port, exc)
            return None

        try:
            ssl_obj = writer.get_extra_info("ssl_object")
            # Get certificate from SSL object instead of peercert extra info
            if ssl_obj is None:
                log.warning(
                    "SSL object missing after successful connection to %s:%d. This may indicate a connection or handshake issue.",
                    domain,
                    port,
                )
                return {"error": f"SSL object missing after successful connection to {domain}:{port}"}
            cert = ssl_obj.getpeercert()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

        if not cert or not isinstance(cert, dict) or len(cert) == 0:
            return None

        certificate = self._normalise_certificate(cert, ssl_obj)
        certificate["port"] = port
        certificate["tls_versions"] = await self._test_tls_versions(domain, port)
        return certificate

    def _normalise_certificate(self, cert: Dict[str, object], ssl_obj: Optional[ssl.SSLObject]) -> Dict[str, object]:
        issuer: Dict[str, object] = {}
        subject: Dict[str, object] = {}
        alt_names: List[str] = []

        for item in cert.get("subject", []):
            key = "_".join(re.findall(".[^A-Z]*", item[0][0])).lower()
            subject[key] = item[0][1]

        for item in cert.get("issuer", []):
            key = "_".join(re.findall(".[^A-Z]*", item[0][0])).lower()
            issuer[key] = item[0][1]

        for item in cert.get("subjectAltName", []):
            alt_names.append(item[1])

        data: Dict[str, object] = {
            "issuer": issuer,
            "subject": subject,
            "subject_alt_names": alt_names,
            "serial": cert.get("serialNumber"),
            "not_before": self._parse_dt(cert.get("notBefore")),
            "not_after": self._parse_dt(cert.get("notAfter")),
            "version": cert.get("version"),
            "handshake_version": ssl_obj.version() if ssl_obj else None,
            "handshake_cipher": ssl_obj.cipher() if ssl_obj else None,
        }

        if cert.get("OCSP"):
            data["ocsp"] = cert["OCSP"][0].strip("/")
        if cert.get("caIssuers"):
            data["ca_issuers"] = cert["caIssuers"][0].strip("/")
        if cert.get("crlDistributionPoints"):
            data["crl_distribution_points"] = cert["crlDistributionPoints"][0].strip("/")

        return data

    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%b %d %H:%M:%S %Y %Z")
        except ValueError:
            return None

    async def _test_tls_versions(self, domain: str, port: int) -> Dict[str, Dict[str, object]]:
        results: Dict[str, Dict[str, object]] = {}
        for label, version in TLS_VERSION_MAP.items():
            res = await self._try_tls_version(domain, port, version)
            results[label] = res
        return results

    async def _try_tls_version(
        self,
        domain: str,
        port: int,
        version: ssl.TLSVersion,
    ) -> Dict[str, object]:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = version
        context.maximum_version = version
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    domain,
                    port,
                    ssl=context,
                    server_hostname=domain,
                ),
                timeout=self.settings.timeout,
            )
            try:
                ssl_obj = writer.get_extra_info("ssl_object")
                cipher = ssl_obj.cipher() if ssl_obj else None
                proto = ssl_obj.version() if ssl_obj else None
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
            return {"accepted": True, "cipher": cipher, "version": proto}
        except Exception:
            return {"accepted": False}

    async def _store_success(
        self,
        domain: str,
        certificate: Dict[str, object],
        timestamp: datetime,
    ) -> None:
        """Store successful SSL certificate scan in PostgreSQL."""
        async with await self.postgres.get_session() as session:
            try:
                # Find or create domain
                stmt = select(Domain).where(Domain.name == domain)
                result = await session.exec(stmt)
                domain_record = result.first()
                
                if not domain_record:
                    domain_record = Domain(
                        name=domain,
                        created_at=timestamp,
                        updated_at=timestamp
                    )
                    session.add(domain_record)
                    await session.flush()  # Get the domain ID

                # Remove existing SSL data to replace it
                if domain_record.id:
                    stmt = select(SSLData).where(SSLData.domain_id == domain_record.id)
                    result = await session.exec(stmt)
                    existing_ssl = result.first()
                    if existing_ssl:
                        # Delete existing alt names
                        stmt = select(SSLSubjectAltName).where(SSLSubjectAltName.ssl_id == existing_ssl.id)
                        result = await session.exec(stmt)
                        for alt_name in result:
                            await session.delete(alt_name)
                        # Delete existing SSL data
                        await session.delete(existing_ssl)

                # Create new SSL data
                issuer = certificate.get("issuer", {})
                subject = certificate.get("subject", {})
                
                ssl_data = SSLData(
                    domain_id=domain_record.id,
                    issuer_common_name=issuer.get("common_name"),
                    issuer_organizational_unit=issuer.get("organizational_unit_name"),
                    issuer_organization=issuer.get("organization_name"),
                    subject_common_name=subject.get("common_name"),
                    subject_organizational_unit=subject.get("organizational_unit_name"),
                    subject_organization=subject.get("organization_name"),
                    serial_number=str(certificate.get("serial")) if certificate.get("serial") else None,
                    ocsp=certificate.get("ocsp"),
                    ca_issuers=certificate.get("ca_issuers"),
                    crl_distribution_points=certificate.get("crl_distribution_points"),
                    not_before=certificate.get("not_before"),
                    not_after=certificate.get("not_after"),
                    updated_at=timestamp,
                )
                session.add(ssl_data)
                await session.flush()  # Get the SSL ID

                # Add subject alternative names
                alt_names = certificate.get("subject_alt_names", [])
                for alt_name in alt_names:
                    if alt_name:
                        ssl_alt_name = SSLSubjectAltName(
                            ssl_id=ssl_data.id,
                            value=str(alt_name)
                        )
                        session.add(ssl_alt_name)

                # Update domain timestamp and clear any failure flags
                domain_record.updated_at = timestamp
                
                await session.commit()
                
            except Exception as exc:
                await session.rollback()
                log.error("Failed to store SSL data for %s: %s", domain, exc)
                raise

    async def _mark_failed(self, domain: str, timestamp: datetime) -> None:
        """Mark SSL scan as failed for a domain."""
        async with await self.postgres.get_session() as session:
            try:
                # Find or create domain
                stmt = select(Domain).where(Domain.name == domain)
                result = await session.exec(stmt)
                domain_record = result.first()
                
                if not domain_record:
                    domain_record = Domain(
                        name=domain,
                        created_at=timestamp,
                        updated_at=timestamp
                    )
                    session.add(domain_record)
                else:
                    domain_record.updated_at = timestamp

                # Note: In PostgreSQL version, we don't have specific SSL failure fields
                # We just update the domain timestamp to mark it as processed
                
                await session.commit()
                
            except Exception as exc:
                await session.rollback()
                log.error("Failed to mark SSL failure for %s: %s", domain, exc)
                raise

    def _log_success(self, domain: str, certificate: Dict[str, object]) -> None:
        log.info(
            "TLS scan success %s (port=%d, not_after=%s)",
            domain,
            certificate.get("port"),
            certificate.get("not_after"),
        )
        if self.settings.verbose_tls:
            log.info("Certificate for %s: %s", domain, certificate)


class SSLConsumer:
    def __init__(self, settings: WorkerSettings, runtime: SSLRuntime) -> None:
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

    async def _handle_job(self, job: SSLJob) -> None:
        try:
            async with self._semaphore:
                await self.runtime.process_domain(job.domain)
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("Unhandled error while scanning %s: %s", job.domain, exc)
        finally:
            with contextlib.suppress(Exception):
                await job.message.ack()


async def get_domains(settings: WorkerSettings) -> AsyncIterator[SSLJob]:
    if not settings.rabbitmq_url:
        raise ValueError("RabbitMQ URL is required to consume TLS jobs")

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max(1, settings.prefetch))
        queue = await channel.declare_queue(settings.queue_name, durable=True)

        async with queue.iterator() as iterator:
            async for message in iterator:
                if message.body == STOP_SENTINEL:
                    yield SSLJob(domain="", message=message, stop=True)
                    break

                domain = message.body.decode().strip()
                if not domain:
                    await message.ack()
                    continue

                yield SSLJob(domain=domain, message=message)
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
    ports: Tuple[int, ...],
    batch_size: int = 10_000,
) -> AsyncIterator[str]:
    """Iterate over domains that need SSL scanning."""
    engine = create_async_engine(postgres_dsn)
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    try:
        async with session_factory() as session:
            # Find domains that have open ports on the specified ports but no SSL data
            stmt = (
                select(Domain.name)
                .join(PortService, Domain.id == PortService.domain_id)
                .outerjoin(SSLData, Domain.id == SSLData.domain_id)
                .where(
                    PortService.port.in_(ports),
                    PortService.status == "open",
                    SSLData.id.is_(None)  # No SSL data exists
                )
                .distinct()
                .limit(batch_size)
            )
            
            result = await session.exec(stmt)
            domains = result.all()
            
            for domain in domains:
                yield domain
    finally:
        await engine.dispose()


async def direct_worker(settings: DirectWorkerSettings) -> str:
    runtime = SSLRuntime(settings)
    engine = create_async_engine(settings.postgres_dsn)
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    try:
        async with session_factory() as session:
            # Find domains that need SSL scanning
            stmt = (
                select(Domain.name)
                .join(PortService, Domain.id == PortService.domain_id)
                .outerjoin(SSLData, Domain.id == SSLData.domain_id)
                .where(
                    PortService.port.in_(settings.ports),
                    PortService.status == "open", 
                    SSLData.id.is_(None)  # No SSL data exists
                )
                .distinct()
                .offset(settings.skip)
                .limit(settings.limit)
            )
            
            result = await session.exec(stmt)
            domains = result.all()

        pending: Set[asyncio.Task[None]] = set()
        semaphore = asyncio.Semaphore(max(1, settings.concurrency))

        for domain in domains:
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
        await engine.dispose()
        await runtime.close()


async def service_worker(settings: WorkerSettings) -> str:
    runtime = SSLRuntime(settings)
    try:
        consumer = SSLConsumer(settings, runtime)
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
@click.option("--postgres-dsn", type=str, default=None, help="PostgreSQL DSN (uses settings if not provided)")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY, show_default=True,
              help="Concurrent TLS scans per worker")
@click.option("--timeout", type=float, default=DEFAULT_TIMEOUT, show_default=True,
              help="TLS handshake timeout in seconds")
@click.option("--ports", type=str, default="443", show_default=True,
              help="Comma-separated list of ports to scan")
@click.option("--rabbitmq-url", "-r", type=str, default=None,
              help="RabbitMQ URL (enables distributed mode)")
@click.option("--queue-name", "-q", type=str, default="ssl_scans", show_default=True,
              help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH, show_default=True,
              help="RabbitMQ prefetch per worker")
@click.option("--purge-queue/--no-purge-queue", default=True,
              help="Purge the queue before enqueuing new domains")
@click.option("--service", is_flag=True,
              help="Run as a RabbitMQ-consuming TLS scan service")
@click.option("--log-tls", is_flag=True,
              help="Log full certificate payloads after each successful scan")
@click.option("--verbose", is_flag=True, help="Enable verbose debug logging")
def main(
    worker: int,
    postgres_dsn: Optional[str],
    concurrency: int,
    timeout: float,
    ports: str,
    rabbitmq_url: Optional[str],
    queue_name: str,
    prefetch: int,
    purge_queue: bool,
    service: bool,
    log_tls: bool,
    verbose: bool,
) -> None:
    """PostgreSQL-backed SSL certificate scanner with RabbitMQ support."""
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    # Use settings or .env file if no explicit DSN provided
    if not postgres_dsn:
        try:
            postgres_dsn = resolve_dsn()
        except ValueError as exc:
            click.echo(f"[ERROR] {exc}")
            click.echo("[INFO] Please set POSTGRES_DSN in environment or .env file")
            click.echo("[INFO] Example: POSTGRES_DSN=postgresql+asyncpg://"
                       "user:pass@localhost:5432/dbname")
            sys.exit(1)
        except Exception as exc:
            click.echo(f"[ERROR] Could not resolve PostgreSQL DSN: {exc}")
            sys.exit(1)

    # Test database connection before proceeding
    async def test_connection():
        try:
            engine = create_async_engine(postgres_dsn)
            session_factory = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            async with session_factory() as session:
                from sqlmodel import text
                result = await session.exec(text("SELECT 1"))
                result.first()
            await engine.dispose()
            return True
        except Exception as exc:
            click.echo(f"[ERROR] Database connection test failed: {exc}")
            click.echo(f"[INFO] DSN: {postgres_dsn}")
            click.echo("[INFO] Please check:")
            click.echo("  - PostgreSQL server is running")
            click.echo("  - Database name, host, port are correct")
            click.echo("  - Username/password are valid")
            click.echo("  - Network connectivity to database server")
            return False

    if not asyncio.run(test_connection()):
        sys.exit(1)

    worker = max(1, worker)
    concurrency = max(1, concurrency)
    prefetch = max(1, prefetch)
    port_tuple = tuple(sorted({int(p.strip()) for p in ports.split(",") if p.strip()})) or DEFAULT_PORTS

    base_settings = WorkerSettings(
        postgres_dsn=postgres_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        concurrency=concurrency,
        timeout=timeout,
        ports=port_tuple,
        log_level=log_level,
        verbose_tls=log_tls,
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

    # Count pending domains
    async def count_pending():
        engine = create_async_engine(postgres_dsn)
        session_factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        try:
            async with session_factory() as session:
                stmt = (
                    select(Domain.name)
                    .join(PortService, Domain.id == PortService.domain_id)
                    .outerjoin(SSLData, Domain.id == SSLData.domain_id)
                    .where(
                        PortService.port.in_(port_tuple),
                        PortService.status == "open",
                        SSLData.id.is_(None)  # No SSL data exists
                    )
                    .distinct()
                )
                result = await session.exec(stmt)
                return len(result.all())
        except Exception as exc:
            log.error("Failed to connect to PostgreSQL: %s", exc)
            click.echo(f"[ERROR] Database connection failed: {exc}")
            click.echo(f"[INFO] PostgreSQL DSN: {postgres_dsn}")
            click.echo("[INFO] Please check your database connection and DSN")
            return 0
        finally:
            await engine.dispose()
    
    total_docs = asyncio.run(count_pending())
    click.echo(f"[INFO] total domains to scan: {total_docs}")

    if total_docs == 0:
        click.echo("[INFO] Nothing to scan")
        return

    if rabbitmq_url:
        domains = iter_pending_domains(postgres_dsn, port_tuple)
        log.info("Publishing TLS scan jobs to RabbitMQ at %s", rabbitmq_url)
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
        click.echo("[INFO] Published TLS scan jobs to RabbitMQ. Start workers with --service")
    else:
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