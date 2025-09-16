#!/usr/bin/env python3
import asyncio
import argparse
import multiprocessing
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import dns.asyncresolver
import dns.exception


CONCURRENCY = 200   # number of in-flight DNS queries per process
BATCH_SIZE = 100    # number of domains claimed from DB at once


async def resolve_record(resolver, domain, rtype):
    """Resolve a DNS record asynchronously, return normalized results or None."""
    try:
        answer = await resolver.resolve(domain, rtype, lifetime=2)
        records = []
        for item in answer:
            if rtype == "A":
                records.append(item.address)
            elif rtype == "AAAA":
                records.append(item.address)
            elif rtype == "NS":
                records.append(item.target.to_unicode().strip(".").lower())
            elif rtype == "SOA":
                records.append(item.to_text().replace("\\", "").lower())
            elif rtype == "CNAME":
                records.append(
                    {"target": item.target.to_unicode().strip(".").lower()})
            elif rtype == "MX":
                records.append({
                    "preference": item.preference,
                    "exchange": item.exchange.to_unicode().lower().strip("."),
                })
        return records
    except (dns.exception.DNSException, Exception):
        return None


async def handle_domain(domain, resolver):
    """Query all record types concurrently and build update ops."""
    record_types = {
        "a_record": "A",
        "aaaa_record": "AAAA",
        "ns_record": "NS",
        "mx_record": "MX",
        "soa_record": "SOA",
        "cname_record": "CNAME",
    }

    tasks = {key: resolve_record(resolver, domain, rtype)
             for key, rtype in record_types.items()}
    results = await asyncio.gather(*tasks.values())

    update = {"updated": datetime.utcnow()}
    for key, recs in zip(tasks.keys(), results):
        if recs:
            update[key] = recs

    if update:
        return UpdateOne({"domain": domain}, {"$set": update}, upsert=True)
    return None


async def worker_loop(mongo_host):
    client = AsyncIOMotorClient(f"mongodb://{mongo_host}:27017")
    db = client.ip_data
    resolver = dns.asyncresolver.Resolver()
    sem = asyncio.Semaphore(CONCURRENCY)

    while True:
        docs = await db.dns.find(
            {"updated": {"$exists": False}, "claimed": {"$ne": True}},
            {"domain": 1},
            limit=BATCH_SIZE,
        ).to_list(length=BATCH_SIZE)

        if not docs:
            break

        ids = [d["_id"] for d in docs]
        await db.dns.update_many({"_id": {"$in": ids}}, {"$set": {"claimed": True}})

        async def bounded_task(doc):
            async with sem:
                return await handle_domain(doc["domain"], resolver)

        tasks = [bounded_task(doc) for doc in docs]
        ops = [op for op in await asyncio.gather(*tasks) if op]

        if ops:
            await db.dns.bulk_write(ops, ordered=False)

    client.close()


def start_process(mongo_host):
    asyncio.run(worker_loop(mongo_host))


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, required=True, help="MongoDB host")
    parser.add_argument("--workers", type=int, required=True,
                        help="Number of processes")
    return parser.parse_args()


if __name__ == "__main__":
    args = argparser()
    print(
        (
            f"INFO: Starting {args.workers} processes, "
            f"each with {CONCURRENCY} concurrent DNS tasks"
        )
    )

    with multiprocessing.Pool(processes=args.workers) as pool:
        pool.map(start_process, [args.host] * args.workers)

    print("INFO: All workers finished")
