#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
High-throughput async DNS resolver + Mongo updater.

Key features:
- Batched, race-safe claiming with TTL (few DB round-trips per batch)
- Streaming concurrency with asyncio.as_completed
- Chunked bulk writes to MongoDB (ordered=False)
- Tuned dnspython async resolver (rotate, no search suffixes, optional local cache)
- Optional JSONL printing of per-domain DNS results (--print-dns)
- Throughput meter (domains/sec)

Recommended indexes (run once in Mongo shell):
  db.dns.createIndex({ domain: 1 }, { unique: true })
  db.dns.createIndex({ updated: 1 })
  db.dns.createIndex({ claimed_until: 1 })
  db.dns.createIndex({ claimed_by: 1, claimed_until: 1 })
"""

import os
import json
import time
import asyncio
import argparse
import platform
import multiprocessing as mp
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import dns.asyncresolver
import dns.exception
import dns.resolver

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import UpdateOne

# -----------------------------------------------------------------------------
# Config (overridable via env)
# -----------------------------------------------------------------------------
# in-flight domains per process
CONCURRENCY = int(os.getenv("DNS_CONCURRENCY", "200"))
# docs to claim per cycle
CLAIM_BATCH = int(os.getenv("CLAIM_BATCH", "200"))
# flush bulk writes every N ops
WRITE_CHUNK = int(os.getenv("WRITE_CHUNK", "500"))
CLAIM_TTL_SEC = int(os.getenv("CLAIM_TTL_SEC", "300")
                    )        # reclaim after 5 min
DNS_LIFETIME = float(os.getenv("DNS_LIFETIME", "2.0")
                     )        # per-query lifetime
DNS_TIMEOUT = float(os.getenv("DNS_TIMEOUT", "1.0"))         # per-try timeout

PRINT_DNS = False  # toggled via CLI

RECORD_TYPES: Dict[str, str] = {
    "a_record": "A",
    "aaaa_record": "AAAA",
    "ns_record": "NS",
    "mx_record": "MX",
    "soa_record": "SOA",
    "cname_record": "CNAME",
}

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(processName)s %(levelname)s: %(message)s",
)
log = logging.getLogger("dns-scan")

# -----------------------------------------------------------------------------
# Throughput meter
# -----------------------------------------------------------------------------
_stats_last = time.monotonic()
_stats_count = 0


async def flush_stats(n_new: int) -> None:
    """Log domains/sec every ~2 seconds."""
    global _stats_last, _stats_count
    _stats_count += n_new
    now = time.monotonic()
    if now - _stats_last >= 2.0:
        rate = _stats_count / (now - _stats_last)
        log.info("Throughput: %.1f domains/sec", rate)
        _stats_last = now
        _stats_count = 0

# -----------------------------------------------------------------------------
# DNS Helpers
# -----------------------------------------------------------------------------


def parse_resolver_arg(resolver_arg: Optional[str]) -> Optional[List[str]]:
    if not resolver_arg:
        return None
    items = [x.strip() for x in resolver_arg.split(",")]
    return [x for x in items if x]


def make_resolver(resolver_ns: Optional[List[str]]) -> dns.asyncresolver.Resolver:
    r = dns.asyncresolver.Resolver()
    if resolver_ns:
        r.nameservers = resolver_ns
    else:
        # Prefer a local caching resolver if available for best performance:
        r.nameservers = ["127.0.0.1"]
        # Fallback: uncomment below if no local cache
        # r.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9", "1.0.0.1", "8.8.4.4"]
    r.timeout = DNS_TIMEOUT
    r.lifetime = DNS_LIFETIME
    r.cache = dns.resolver.Cache()
    r.rotate = True
    r.search = []
    return r


async def resolve_record(
    resolver: dns.asyncresolver.Resolver,
    domain: str,
    rtype: str,
) -> Optional[List[Any]]:
    try:
        ans = await resolver.resolve(domain, rtype, lifetime=DNS_LIFETIME)
        out: List[Any] = []
        for item in ans:
            if rtype in {"A", "AAAA"}:
                out.append(item.address)
            elif rtype == "NS":
                out.append(item.target.to_unicode().strip(".").lower())
            elif rtype == "SOA":
                out.append(item.to_text().replace("\\", "").lower())
            elif rtype == "CNAME":
                out.append(
                    {"target": item.target.to_unicode().strip(".").lower()})
            elif rtype == "MX":
                out.append({
                    "preference": int(item.preference),
                    "exchange": item.exchange.to_unicode().strip(".").lower(),
                })
        return out or None
    except (dns.exception.DNSException, Exception):
        return None


def format_dns_result(domain: str, update: Dict[str, Any]) -> str:
    payload = {"ts": datetime.utcnow().isoformat(), "domain": domain}
    for k in ("a_record", "aaaa_record", "ns_record", "mx_record", "soa_record", "cname_record"):
        if k in update:
            payload[k] = update[k]
    return json.dumps(payload, default=str, ensure_ascii=False)


async def handle_domain(
    domain: str,
    resolver: dns.asyncresolver.Resolver,
) -> UpdateOne:
    # Launch all record queries concurrently for this domain
    tasks = {k: resolve_record(resolver, domain, v)
             for k, v in RECORD_TYPES.items()}
    results = await asyncio.gather(*tasks.values(), return_exceptions=False)

    update: Dict[str, Any] = {"updated": datetime.utcnow()}
    for key, recs in zip(tasks.keys(), results):
        if recs:
            update[key] = recs

    if PRINT_DNS:
        print(format_dns_result(domain, update))

    # Always at least sets 'updated'
    return UpdateOne({"domain": domain}, {"$set": update}, upsert=True)


# -----------------------------------------------------------------------------
# Claiming (batched, race-safe)
# -----------------------------------------------------------------------------
async def claim_batch_fast(
    db: AsyncIOMotorDatabase,
    worker_id: str,
    batch_size: int,
) -> List[Dict[str, Any]]:  # returns [{_id, domain}, ...]
    now = datetime.utcnow()
    expiry = now + timedelta(seconds=CLAIM_TTL_SEC)

    # 1) Preselect candidates (no round-trip per doc)
    cursor = db.dns.find(
        {
            "updated": {"$exists": False},
            "$or": [
                {"claimed_until": {"$exists": False}},
                {"claimed_until": {"$lte": now}},
            ],
        },
        {"_id": 1, "domain": 1},
        limit=batch_size,
    )
    docs = await cursor.to_list(length=batch_size)
    if not docs:
        return []

    ids = [d["_id"] for d in docs]

    # 2) Atomically claim only those still eligible
    await db.dns.update_many(
        {
            "_id": {"$in": ids},
            "updated": {"$exists": False},
            "$or": [
                {"claimed_until": {"$exists": False}},
                {"claimed_until": {"$lte": now}},
            ],
        },
        {"$set": {"claimed_by": worker_id, "claimed_until": expiry}},
    )

    # 3) Read back only what we actually claimed
    claimed = await db.dns.find(
        {"_id": {"$in": ids}, "claimed_by": worker_id},
        {"domain": 1}
    ).to_list(length=batch_size)

    return claimed


async def release_claims(db: AsyncIOMotorDatabase, ids: List[Any]) -> None:
    if not ids:
        return
    await db.dns.update_many(
        {"_id": {"$in": ids}},
        {"$unset": {"claimed_by": "", "claimed_until": ""}},
    )


# -----------------------------------------------------------------------------
# Worker
# -----------------------------------------------------------------------------
async def worker_loop(
    mongo_uri: str,
    db_name: str,
    worker_id: str,
    resolver_ns: Optional[List[str]],
) -> None:
    client = AsyncIOMotorClient(
        mongo_uri,
        maxPoolSize=100,
        connectTimeoutMS=5000,
        socketTimeoutMS=15000,
        serverSelectionTimeoutMS=5000,
        retryWrites=True,
    )
    db: AsyncIOMotorDatabase = client[db_name]
    resolver = make_resolver(resolver_ns)
    sem = asyncio.Semaphore(CONCURRENCY)

    try:
        while True:
            docs = await claim_batch_fast(db, worker_id, CLAIM_BATCH)
            if not docs:
                log.info("No more unprocessed docs. Worker %s exiting.", worker_id)
                break

            ops_buffer: List[UpdateOne] = []
            claimed_ids = [d["_id"] for d in docs]

            async def bounded(doc: Dict[str, Any]) -> Optional[UpdateOne]:
                async with sem:
                    return await handle_domain(doc["domain"], resolver)

            tasks = [asyncio.create_task(bounded(doc)) for doc in docs]

            # Stream results; flush writes in chunks
            for coro in asyncio.as_completed(tasks):
                op = await coro
                if op:
                    ops_buffer.append(op)
                    if len(ops_buffer) >= WRITE_CHUNK:
                        try:
                            await db.dns.bulk_write(
                                ops_buffer,
                                ordered=False,
                                bypass_document_validation=True,
                            )
                            await flush_stats(len(ops_buffer))
                        except Exception:
                            log.exception("Bulk write chunk failed")
                        finally:
                            ops_buffer.clear()

            # Flush remaining
            if ops_buffer:
                try:
                    await db.dns.bulk_write(
                        ops_buffer,
                        ordered=False,
                        bypass_document_validation=True,
                    )
                    await flush_stats(len(ops_buffer))
                except Exception:
                    log.exception("Final bulk write failed")
                finally:
                    ops_buffer.clear()

            # Clean up claim markers
            await release_claims(db, claimed_ids)

    finally:
        client.close()


def start_process(mongo_uri: str, db_name: str, print_dns: bool, resolver_arg: Optional[str]) -> None:
    global PRINT_DNS
    PRINT_DNS = print_dns
    resolver_ns = parse_resolver_arg(resolver_arg)

    # Optional uvloop for speed (Linux/macOS)
    try:
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass

    worker_id = f"pid-{os.getpid()}"
    asyncio.run(worker_loop(mongo_uri, db_name, worker_id, resolver_ns))

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Async DNS resolver with MongoDB updater")
    p.add_argument("--uri", type=str, required=True,
                   help="MongoDB URI (e.g. mongodb://host:27017)")
    p.add_argument("--db", type=str, required=True, help="Database name")
    p.add_argument("--workers", type=int, required=True,
                   help="Number of worker processes")
    p.add_argument("--print-dns", action="store_true",
                   help="Print per-domain DNS JSONL to stdout")
    p.add_argument(
        "--resolver",
        type=str,
        default=None,
        help="Comma-separated DNS servers (e.g. '127.0.0.1' or '1.1.1.1,8.8.8.8'). "
             "Defaults to 127.0.0.1 (local caching resolver).",
    )
    return p.parse_args()


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    args = parse_args()

    # Prefer 'fork' on Linux for lower process start overhead; 'spawn' elsewhere
    method = "fork" if platform.system() == "Linux" else "spawn"
    ctx = mp.get_context(method)

    log.info(
        "Starting %s workers | concurrency=%s | claim_batch=%s | write_chunk=%s | resolver=%s",
        args.workers, CONCURRENCY, CLAIM_BATCH, WRITE_CHUNK, args.resolver or "127.0.0.1",
    )

    procs = [
        ctx.Process(
            target=start_process,
            args=(args.uri, args.db, args.print_dns, args.resolver),
            daemon=False,
        )
        for _ in range(args.workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    log.info("All workers finished.")
