#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import idna
import math
import asyncio
import multiprocessing
import click
import httpx

from fake_useragent import UserAgent
from lxml import html
from lxml.etree import ParserError, XMLSyntaxError
from urllib.parse import urljoin, urlparse
from datetime import datetime

from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import (
    DuplicateKeyError,
    AutoReconnect,
    WriteError,
    CursorNotFound,
)


# -----------------------------
# MongoDB
# -----------------------------
def connect_async(host: str):
    client = AsyncIOMotorClient(f"mongodb://{host}:27017")
    return client.url_data, client.ip_data


async def retrieve_domains(db_ip_data, skip: int, limit: int):
    cursor = db_ip_data.dns.find(
        {"domain_crawled": {"$exists": False}},
        sort=[("$natural", 1)],
        skip=skip,
        limit=limit,
    )
    return [doc async for doc in cursor]


async def update_data(db_ip_data, domain: str, failed: bool = False):
    """Mark domain as crawled or failed."""
    field = "crawl_failed" if failed else "domain_crawled"
    try:
        res = await db_ip_data.dns.update_one(
            {"domain": domain},
            {"$set": {field: datetime.utcnow()}},
            upsert=False,
        )
        if res.modified_count > 0:
            print(
                f"[INFO] domain {domain} {'FAILED' if failed else 'updated'}")
    except DuplicateKeyError:
        pass


async def add_urls(db_url_data, url: str):
    try:
        post = {"url": url.lower(), "created": datetime.utcnow()}
        res = await db_url_data.url.insert_one(post)
        print(f"[INFO] added url {url} with id {res.inserted_id}")
    except AutoReconnect:
        await asyncio.sleep(5)
    except (DuplicateKeyError, WriteError) as e:
        print(f"[WARN] {e}")


# -----------------------------
# HTTP crawling
# -----------------------------
async def get_urls(ua, url):
    # Try to convert to punycode (IDNA)
    try:
        safe_domain = idna.encode(url).decode("ascii")
    except idna.IDNAError:
        click.echo(f"[WARN] Skipping invalid domain: {url}")
        return None

    headers = {"User-Agent": ua.chrome}
    base_url = f"http://{safe_domain}"
    url_set = set()

    try:
        async with httpx.AsyncClient(
            timeout=5,
            follow_redirects=True
        ) as client:
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
async def async_worker(host: str, skip: int, limit: int):
    db_url_data, db_ip_data = connect_async(host)
    ua = UserAgent()

    try:
        domains = await retrieve_domains(db_ip_data, skip, limit)
    except CursorNotFound:
        return f"Worker {skip}:{skip+limit} cursor error"

    for doc in domains:
        d = doc["domain"]
        click.echo(f"[INFO] processing domain {d}")

        links = await get_urls(ua, d)  # your pure function

        if links is None:
            # invalid IDNA host or fetch/parsing error
            await update_data(db_ip_data, d, failed=True)
            continue

        if links:
            await asyncio.gather(
                *(add_urls(db_url_data, link) for link in links),
                return_exceptions=True
            )

        # mark as crawled even if links was an empty set
        await update_data(db_ip_data, d, failed=False)

    return f"Worker {skip}:{skip+limit} done ({len(domains)} domains)"


def run_worker(task):
    host, skip, limit = task
    return asyncio.run(async_worker(host, skip, limit))


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
def main(worker, host):
    sync_client = MongoClient(f"mongodb://{host}:27017")
    total_docs = sync_client.ip_data.dns.count_documents(
        {"domain_crawled": {"$exists": False}}
    )
    sync_client.close()

    click.echo(f"[INFO] total documents to process: {total_docs}")

    chunk_size = math.ceil(total_docs / worker)
    tasks = [(host, i * chunk_size, chunk_size) for i in range(worker)]

    with multiprocessing.Pool(processes=worker) as pool:
        for result in pool.imap_unordered(run_worker, tasks):
            click.echo(result)


if __name__ == "__main__":
    main()
