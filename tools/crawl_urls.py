#!/usr/bin/env python3
import asyncio
import logging
import math
import multiprocessing
from datetime import datetime
from typing import Iterable, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

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


log = logging.getLogger(__name__)


# -----------------------------
# MongoDB
# -----------------------------
def connect_async(host: str):
    client = AsyncIOMotorClient(f"mongodb://{host}:27017", tz_aware=True)
    return client.url_data, client.ip_data


def iter_pending_domains(sync_client: MongoClient, batch_size: int = 5000):
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


async def update_data(db_ip_data, domain: str, failed: bool = False):
    """Mark domain as crawled or failed."""
    field = "crawl_failed" if failed else "domain_crawled"
    try:
        res = await db_ip_data.dns.update_one(
            {"domain": domain},
            {"$set": {field: datetime.now()}},
            upsert=False,
        )
        if res.modified_count > 0:
            print(
                f"[INFO] domain {domain} {'FAILED' if failed else 'updated'}")
    except DuplicateKeyError:
        pass


def _build_user_agent() -> str:
    try:
        return UserAgent().chrome
    except Exception as exc:
        log.debug("Falling back to static user agent: %s", exc)
        return "Mozilla/5.0 (compatible; purple_jo/1.0)"


async def add_urls(db_url_data, urls: Iterable[str]):
    documents = [{"url": url.lower(), "created": datetime.now()}
                 for url in urls]
    log.info("Inserting %d new URLs", documents)
    if not documents:
        return

    try:
        await db_url_data.url.insert_many(documents, ordered=False)
    except BulkWriteError as exc:
        # ignore duplicate key errors, surface anything else
        write_errors = exc.details.get(
            "writeErrors", []) if exc.details else []
        non_duplicate = [
            err for err in write_errors if err.get("code") != 11000]
        if non_duplicate:
            log.warning(
                "Bulk insert encountered non-duplicate errors: %s",
                non_duplicate
            )
    except AutoReconnect:
        await asyncio.sleep(5)
    except (DuplicateKeyError, WriteError) as err:
        log.warning("Insert failed: %s", err)


# -----------------------------
# HTTP crawling
# -----------------------------
async def get_urls(
    client: httpx.AsyncClient,
    user_agent: str,
    url: str
) -> Optional[Set[str]]:
    # Try to convert to punycode (IDNA)
    try:
        safe_domain = idna.encode(url).decode("ascii")
    except idna.IDNAError:
        click.echo(f"[WARN] Skipping invalid domain: {url}")
        return None

    headers = {"User-Agent": user_agent}
    base_url = f"http://{safe_domain}"
    url_set = set()

    try:
        res = await client.get(base_url, headers=headers)
        content = res.text
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        click.echo(f"[WARN] Failed to fetch {base_url}: {e}")
        return None

    try:
        doc = html.document_fromstring(content)
    except (ValueError, ParserError, XMLSyntaxError):
        return None

    links = doc.xpath("//a/@href")
    for link in links:
        link = link.lower().strip()
        if link.startswith(("#", "+", "tel:", "javascript:", "mailto:")):
            continue
        elif link.startswith(("/", "?", "..")):
            link = urljoin(base_url, link)

        try:
            parsed = urlparse(link)
            if parsed.netloc:
                url_set.add(link)
        except ValueError:
            continue

    return url_set


# -----------------------------
# Worker
# -----------------------------
async def process_domain(
    db_url_data,
    db_ip_data,
    client,
    user_agent: str,
    domain: str
) -> Tuple[str, bool]:
    click.echo(f"[INFO] processing domain {domain}")

    links = await get_urls(client, user_agent, domain)
    if links is None:
        await update_data(db_ip_data, domain, failed=True)
        return domain, False

    if links:
        await add_urls(db_url_data, links)
        log.info("Captured %d links from %s", len(links), domain)

    await update_data(db_ip_data, domain, failed=False)
    return domain, True


async def _bounded_process(semaphore: asyncio.Semaphore, coro):
    await semaphore.acquire()
    try:
        return await coro
    finally:
        semaphore.release()


def build_domain_cursor(db_ip_data, skip: int, limit: int):
    return db_ip_data.dns.find(
        {"domain_crawled": {"$exists": False}},
        {"domain": 1},
        sort=[("$natural", 1)],
        skip=skip,
        limit=limit,
    )


async def async_worker_direct(host: str, skip: int, limit: int, concurrency: int):
    db_url_data, db_ip_data = connect_async(host)
    user_agent = _build_user_agent()

    cursor = build_domain_cursor(db_ip_data, skip, limit)

    processed = 0
    successes = 0
    semaphore = asyncio.Semaphore(max(1, concurrency))
    tasks = []
    cursor_error = None

    async with httpx.AsyncClient(timeout=5, follow_redirects=True) as client:
        try:
            async for doc in cursor:
                domain = doc.get("domain")
                if not domain:
                    continue
                task = asyncio.create_task(
                    _bounded_process(
                        semaphore,
                        process_domain(
                            db_url_data, db_ip_data, client, user_agent, domain
                        ),
                    )
                )
                tasks.append(task)
        except CursorNotFound:
            cursor_error = f"Worker {skip}:{skip+limit} cursor error"

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    log.warning("Domain task failed: %s", result)
                    continue
                processed += 1
                if result[1]:
                    successes += 1

    if cursor_error:
        return cursor_error

    return (
        f"Worker {skip}:{skip+limit} done "
        f"({processed} domains, success={successes})"
    )


def run_worker_direct(task):
    host, skip, limit, concurrency = task
    return asyncio.run(async_worker_direct(host, skip, limit, concurrency))


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

        for domain in domains:
            message = Message(
                body=domain.encode("utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
            )
            await channel.default_exchange.publish(message, routing_key=queue_name)
            sent += 1

        for _ in range(worker_count):
            stop_message = Message(
                body=b"__STOP__",
                delivery_mode=DeliveryMode.PERSISTENT,
            )
            await channel.default_exchange.publish(
                stop_message, routing_key=queue_name)

    return sent


async def queue_consumer(
    host: str,
    rabbitmq_url: str,
    queue_name: str,
    concurrency: int,
    prefetch: int,
):
    db_url_data, db_ip_data = connect_async(host)
    user_agent = _build_user_agent()
    processed = 0
    successes = 0
    semaphore = asyncio.Semaphore(max(1, concurrency))

    connection = await aio_pika.connect_robust(rabbitmq_url)

    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max(1, prefetch))
        queue = await channel.declare_queue(queue_name, durable=True)

        async with httpx.AsyncClient(timeout=5, follow_redirects=True) as client:
            pending = set()

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    domain = message.body.decode()

                    if domain == "__STOP__":
                        await message.ack()
                        break

                    async def handle(msg, domain_name):
                        nonlocal processed, successes
                        try:
                            async with semaphore:
                                result = await process_domain(
                                    db_url_data,
                                    db_ip_data,
                                    client,
                                    user_agent,
                                    domain_name,
                                )
                        except Exception as exc:  # pragma: no cover - best effort logging
                            log.warning(
                                "Domain %s failed during crawl: %s",
                                domain_name,
                                exc,
                            )
                            await update_data(db_ip_data, domain_name, failed=True)
                            processed += 1
                            await msg.ack()
                            return

                        await msg.ack()
                        processed += 1
                        if result[1]:
                            successes += 1

                    task = asyncio.create_task(handle(message, domain))
                    pending.add(task)
                    task.add_done_callback(pending.discard)

            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
    finally:
        await connection.close()

    summary = f"Worker done ({processed} domains, success={successes})"
    log.info(summary)
    return summary


def run_worker(task):
    host, rabbitmq_url, queue_name, concurrency, prefetch = task
    try:
        return asyncio.run(
            queue_consumer(host, rabbitmq_url, queue_name, concurrency, prefetch)
        )
    except AMQPConnectionError as exc:
        return (
            f"[ERROR] Worker could not connect to RabbitMQ at {rabbitmq_url}: {exc}"
        )


# -----------------------------
# CLI with Click
# -----------------------------
@click.command()
@click.option(
    "--worker", "-w",
    type=int,
    required=True,
    help="Number of processes"
)
@click.option("--host", "-h", type=str, required=True, help="Mongo host")
@click.option(
    "--concurrency", "-c",
    type=int,
    default=10,
    show_default=True,
    help="Concurrent domains processed per worker"
)
@click.option(
    "--rabbitmq-url", "-r",
    type=str,
    default=None,
    help="RabbitMQ connection URL (e.g. amqp://guest:guest@localhost/). If omitted, the legacy multiprocessing mode is used."
)
@click.option(
    "--queue-name", "-q",
    type=str,
    default="crawl_domains",
    show_default=True,
    help="Name of the RabbitMQ queue"
)
@click.option(
    "--prefetch",
    type=int,
    default=100,
    show_default=True,
    help="RabbitMQ prefetch per worker"
)
@click.option(
    "--purge-queue/--no-purge-queue",
    default=True,
    help="Clear the queue before publishing domains"
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose debug logging"
)
def main(worker, host, concurrency, rabbitmq_url, queue_name, prefetch, purge_queue, verbose):
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level,
                        format="[%(levelname)s] %(message)s")

    worker_count = max(1, worker)
    concurrency = max(1, concurrency)
    prefetch = max(1, prefetch)

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
        try:
            published = asyncio.run(
                enqueue_domains(
                    rabbitmq_url=rabbitmq_url,
                    queue_name=queue_name,
                    domains=domains,
                    worker_count=worker_count,
                    purge_queue=purge_queue,
                )
            )
        except AMQPConnectionError as exc:
            sync_client.close()
            raise click.ClickException(
                f"Failed to connect to RabbitMQ at {rabbitmq_url}: {exc}. "
                "Ensure the broker is running and reachable."
            ) from exc

        click.echo(
            f"[INFO] queued {published} domains onto RabbitMQ queue '{queue_name}'"
        )

        sync_client.close()

        worker_args = [
            (host, rabbitmq_url, queue_name, concurrency, prefetch)
            for _ in range(worker_count)
        ]

        with multiprocessing.Pool(processes=worker_count) as pool:
            log.info("Spawned %d worker processes", worker_count)
            for result in pool.imap_unordered(run_worker, worker_args):
                click.echo(result)
    else:
        sync_client.close()
        chunk_size = math.ceil(total_docs / worker_count) if total_docs else 0

        tasks = []
        start = 0
        while start < total_docs:
            tasks.append(
                (host, start, min(chunk_size, total_docs - start), concurrency)
            )
            start += chunk_size or total_docs

        if not tasks:
            click.echo("[INFO] Nothing to crawl")
            return

        with multiprocessing.Pool(processes=worker_count) as pool:
            for result in pool.imap_unordered(run_worker_direct, tasks):
                click.echo(result)


if __name__ == "__main__":
    main()
