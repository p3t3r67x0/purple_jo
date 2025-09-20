#!/usr/bin/env python3
import asyncio
import httpx
import argparse
import multiprocessing
from datetime import datetime
from fake_useragent import UserAgent
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne


BATCH_SIZE = 100
CONCURRENCY = 200   # requests in flight per process


async def fetch_header(client, ua, domain):
    url = f"http://{domain}"
    try:
        r = await client.head(
            url,
            timeout=2.0,
            follow_redirects=False,
            headers={"User-Agent": ua.random},
        )
        http = {"version": str(r.http_version)}
        status = {"status": str(r.status_code)}
        headers = {k.lower(): v for k, v in r.headers.items()}
        headers.update(status)
        headers.update(http)
        print(
            f"[{multiprocessing.current_process().name}] fetched {domain} ({r.status_code})")
        return {"domain": domain, "header": headers, "updated": datetime.now()}
    except Exception:
        return {"domain": domain, "header_scan_failed": datetime.now()}


async def process_batch(db, client, ua, docs):
    tasks = [fetch_header(client, ua, doc["domain"]) for doc in docs]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    ops = [UpdateOne({"domain": res["domain"]}, {"$set": res})
           for res in results]

    if ops:
        await db.dns.bulk_write(ops, ordered=False)


async def worker_loop(mongo_host):
    client = AsyncIOMotorClient(f"mongodb://{mongo_host}:27017")
    db = client.ip_data
    ua = UserAgent()

    async with httpx.AsyncClient(http2=True) as http_client:
        sem = asyncio.Semaphore(CONCURRENCY)

        while True:
            docs = await db.dns.find(
                {
                    "header": {"$exists": False},
                    "header_scan_failed": {"$exists": False},
                    "ports.port": {"$in": [80, 443]},
                    "claimed_header_scan": {"$exists": False},
                },
                {"domain": 1},
                limit=BATCH_SIZE,
            ).to_list(length=BATCH_SIZE)

            if not docs:
                break

            ids = [d["_id"] for d in docs]
            await db.dns.update_many({"_id": {"$in": ids}}, {"$set": {"claimed_header_scan": True}})

            async def bounded_process(doc):
                async with sem:
                    return await fetch_header(http_client, ua, doc["domain"])

            tasks = [bounded_process(doc) for doc in docs]
            results = await asyncio.gather(*tasks)
            ops = [UpdateOne({"domain": res["domain"]}, {"$set": res})
                   for res in results]

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
        f"INFO: Starting {args.workers} processes, each with {CONCURRENCY} concurrent requests")

    with multiprocessing.Pool(processes=args.workers) as pool:
        pool.map(start_process, [args.host] * args.workers)

    print("INFO: All workers finished")
