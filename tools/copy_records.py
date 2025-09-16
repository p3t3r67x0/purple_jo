#!/usr/bin/env python3
import argparse
import asyncio
import multiprocessing
from motor.motor_asyncio import AsyncIOMotorClient

from utils.extract_geodata import read_dataframe
from utils.update_entry import handle_query


async def connect(host: str):
    """Async connection to MongoDB."""
    return AsyncIOMotorClient(f"mongodb://{host}:27017").ip_data


async def retrieve_mx_records(db, skip: int, limit: int):
    """Retrieve domains with MX records."""
    cursor = db.dns.find(
        {"mx_record.exchange": {"$exists": True},
            "mx_scan_failed": {"$exists": False}},
        {"_id": 0, "mx_record.exchange": 1},
        skip=skip,
        limit=limit,
    )
    return [doc async for doc in cursor]


async def retrieve_domain(db, domain: str):
    """Retrieve domain details."""
    return await db.dns.find_one({"domain": domain})


async def worker(df, host: str, skip: int, limit: int):
    """Async worker scanning MX records in range."""
    db = await connect(host)
    mx_records_uniq = set()

    mx_records = await retrieve_mx_records(db, skip, limit)

    for mx_record in mx_records:
        for mx in mx_record.get("mx_record", []):
            mx_records_uniq.add(mx["exchange"])

    tasks = []
    for mx in mx_records_uniq:
        tasks.append(handle_mx(db, df, mx))

    await asyncio.gather(*tasks)


async def handle_mx(db, df, mx):
    """Check if MX domain exists, otherwise process it."""
    data = await retrieve_domain(db, mx)
    if not data:
        # handle_query is synchronous → run it in thread pool
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, handle_query, mx, df, "mx_record.exchange", "mx_scan_failed")


def run_worker(df, host: str, skip: int, limit: int):
    """Entry point for multiprocessing worker."""
    asyncio.run(worker(df, host, skip, limit))


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, required=True,
                        help="number of processes")
    parser.add_argument("--host", type=str, required=True, help="MongoDB host")
    return parser.parse_args()


if __name__ == "__main__":
    args = argparser()
    df = read_dataframe("data/geodata.csv")

    # Use one sync client to get total count (Motor not needed here)
    client = AsyncIOMotorClient(f"mongodb://{args.host}:27017")
    db = client.ip_data
    total = db.dns.estimated_document_count()

    # Divide work
    amount = max(1, round(total / (args.workers + 5000)))
    ranges = [(i * amount, amount) for i in range(args.workers)]

    print(f"[INFO] Spawning {args.workers} workers, batch size {amount}")

    with multiprocessing.Pool(processes=args.workers) as pool:
        pool.starmap(run_worker, [(df, args.host, skip, limit)
                     for skip, limit in ranges])

    print("[INFO] All workers finished")
