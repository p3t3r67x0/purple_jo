#!/usr/bin/env python3
"""Distributed TLS certificate scanner backed by RabbitMQ and MongoDB."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import multiprocessing
import os
import re
import ssl
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError
import click
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient


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


class MongoAsync:
    def __init__(self, host: str) -> None:
        self._client = AsyncIOMotorClient(f"mongodb://{host}:27017", tz_aware=True)
        self.db = self._client.ip_data

    def close(self) -> None:
        self._client.close()


class SSLRuntime:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.mongo = MongoAsync(settings.mongo_host)
        self.stats = ScanStats()

    async def close(self) -> None:
        self.mongo.close()

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
        ssl_ctx.verify_mode = ssl.CERT_NONE

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
            cert = writer.get_extra_info("peercert")
            ssl_obj = writer.get_extra_info("ssl_object")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

        if not cert:
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
        await self.mongo.db.dns.update_one(
            {"domain": domain},
            {
                "$set": {"ssl": certificate, "updated": timestamp},
                "$unset": {"ssl_scan_failed": "", "cert_scan_failed": ""},
            },
            upsert=True,
        )

    async def _mark_failed(self, domain: str, timestamp: datetime) -> None:
        await self.mongo.db.dns.update_one(
            {"domain": domain},
            {
                "$set": {"ssl_scan_failed": timestamp},
                "$setOnInsert": {"created": timestamp, "domain": domain},
            },
            upsert=True,
        )

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
    sync_client: MongoClient,
    ports: Tuple[int, ...],
    batch_size: int = 10_000,
) -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()

    def _iterator() -> List[str]:
        results: List[str] = []
        cursor = sync_client.ip_data.dns.find(
            {
                "ssl": {"$exists": False},
                "ssl_scan_failed": {"$exists": False},
                "cert_scan_failed": {"$exists": False},
                "ports": {
                    "$elemMatch": {
                        "port": {"$in": list(ports)},
                        "status": "open",
                        "proto": "tcp",
                    }
                },
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
    runtime = SSLRuntime(settings)
    try:
        cursor = runtime.mongo.db.dns.find(
            {
                "ssl": {"$exists": False},
                "ssl_scan_failed": {"$exists": False},
                "cert_scan_failed": {"$exists": False},
                "ports": {
                    "$elemMatch": {
                        "port": {"$in": settings.ports},
                        "status": "open",
                        "proto": "tcp",
                    }
                },
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
@click.option("--host", "-h", type=str, required=True, help="MongoDB host")
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
    host: str,
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
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    worker = max(1, worker)
    concurrency = max(1, concurrency)
    prefetch = max(1, prefetch)
    port_tuple = tuple(sorted({int(p.strip()) for p in ports.split(",") if p.strip()})) or DEFAULT_PORTS

    base_settings = WorkerSettings(
        mongo_host=host,
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

    sync_client = MongoClient(f"mongodb://{host}:27017", tz_aware=True)
    pending_filter = {
        "ssl": {"$exists": False},
        "ssl_scan_failed": {"$exists": False},
        "cert_scan_failed": {"$exists": False},
        "ports": {
            "$elemMatch": {
                "port": {"$in": list(port_tuple)},
                "status": "open",
                "proto": "tcp",
            }
        },
    }
    total_docs = sync_client.ip_data.dns.count_documents(pending_filter)
    click.echo(f"[INFO] total domains to scan: {total_docs}")

    if total_docs == 0:
        sync_client.close()
        click.echo("[INFO] Nothing to scan")
        return

    if rabbitmq_url:
        domains = iter_pending_domains(sync_client, port_tuple)
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
        sync_client.close()
        click.echo("[INFO] Published TLS scan jobs to RabbitMQ. Start workers with --service")
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
