#!/usr/bin/env python3

"""Bulk GeoIP enrichment for DNS documents stored in MongoDB."""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import platform
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from geoip2 import database
from geoip2.errors import AddressNotFoundError
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

log = logging.getLogger("extract_geoip")


def connect(host: str) -> MongoClient:
    """Create a Mongo client with timezone-aware datetimes."""

    return MongoClient(f"mongodb://{host}:27017", tz_aware=True)


def build_query() -> Dict[str, Any]:
    """Return the base query for documents missing GeoIP data."""

    return {
        "a_record.0": {"$exists": True},
        "country_code": {"$exists": False},
        "geo_lookup_failed": {"$exists": False},
    }


def _sanitize_coordinates(lon: float | None, lat: float | None) -> List[float | None]:
    lon_val = round(lon, 5) if lon is not None else None
    lat_val = round(lat, 5) if lat is not None else None
    return [lon_val, lat_val]


def update_geo_fields(collection: Collection, ip: str, post: Dict[str, Any]) -> None:
    """Persist GeoIP data for a single IP, mirroring legacy schema."""

    updated = datetime.now(timezone.utc)
    set_fields: Dict[str, Any] = {
        "geo": post,
        "country_code": post.get("country_code"),
        "country": post.get("country"),
        "state": post.get("state"),
        "city": post.get("city"),
        "loc": post.get("loc"),
        "updated": updated,
    }

    try:
        res = collection.update_one(
            {"a_record": {"$in": [ip]}},
            {
                "$set": set_fields,
                "$unset": {"geo_lookup_failed": "", "geo_lookup_started": ""},
            },
            upsert=False,
        )
        if res.modified_count > 0:
            log.info("Updated GeoIP for %s -> %s",
                     ip, post.get("country_code"))
    except DuplicateKeyError:
        pass


def mark_geoip_failure(collection: Collection, ip: str) -> None:
    collection.update_one(
        {"a_record": {"$in": [ip]}},
        {
            "$set": {"geo_lookup_failed": datetime.now(timezone.utc)},
            "$unset": {"geo_lookup_started": ""},
        },
        upsert=False,
    )


def mark_geoip_failure_by_id(collection: Collection, doc_id: Any) -> None:
    collection.update_one(
        {"_id": doc_id},
        {
            "$set": {"geo_lookup_failed": datetime.now(timezone.utc)},
            "$unset": {"geo_lookup_started": ""},
        },
        upsert=False,
    )


def handle_ip(collection: Collection, reader: database.Reader, ip: str) -> None:
    try:
        response = reader.city(ip)
    except AddressNotFoundError:
        log.warning("IP not found in GeoIP database: %s", ip)
        mark_geoip_failure(collection, ip)
        return

    post = {
        "country_code": response.country.iso_code,
        "country": response.country.name,
        "state": response.subdivisions.most_specific.name,
        "city": response.city.name,
        "loc": {"type": "Point", "coordinates": _sanitize_coordinates(response.location.longitude, response.location.latitude)},
    }

    update_geo_fields(collection, ip, post)


def claim_next_domain(
    collection: Collection,
    claim_timeout: Optional[int],
) -> Optional[Dict[str, Any]]:
    """Atomically claim the next document needing GeoIP enrichment."""

    now = datetime.now(timezone.utc)
    base_query = build_query()

    if claim_timeout:
        expire_before = now - timedelta(minutes=claim_timeout)
        claim_clause: Dict[str, Any] = {
            "$or": [
                {"geo_lookup_started": {"$exists": False}},
                {"geo_lookup_started": {"$lt": expire_before}},
            ]
        }
    else:
        claim_clause = {"geo_lookup_started": {"$exists": False}}

    query = {"$and": [base_query, claim_clause]}

    return collection.find_one_and_update(
        query,
        {"$set": {"geo_lookup_started": now}},
        sort=[("_id", 1)],
        projection={"a_record": 1},
        return_document=ReturnDocument.BEFORE,
    )


def worker(host: str, input_file: str, claim_timeout: Optional[int]) -> None:
    process_name = mp.current_process().name
    log.info("Worker %s starting", process_name)

    client = connect(host)
    try:
        db: Database = client.ip_data
        collection = db.dns
        with database.Reader(input_file) as reader:
            idle_iterations = 0
            while True:
                domain = claim_next_domain(collection, claim_timeout)
                if not domain:
                    idle_iterations += 1
                    if idle_iterations >= 5:
                        log.info(
                            "Worker %s found no more domains; exiting", process_name)
                        break
                    time.sleep(1)
                    continue

                idle_iterations = 0
                doc_id = domain.get("_id")
                ips = domain.get("a_record", []) or []
                if not ips:
                    if doc_id is not None:
                        mark_geoip_failure_by_id(collection, doc_id)
                    continue
                for ip in ips:
                    handle_ip(collection, reader, ip)
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich Mongo DNS documents with GeoIP data")
    parser.add_argument("--input", required=True, type=Path,
                        help="Path to GeoIP2 City database (mmdb)")
    parser.add_argument("--host", default="localhost", type=str,
                        help="MongoDB host (default: localhost)")
    parser.add_argument("--workers", type=int, default=0,
                        help="Number of worker processes (0 = CPU count)")
    parser.add_argument("--claim-timeout", type=int, default=30,
                        help="Minutes before an in-progress claim is retried (default: 30)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="[%(levelname)s] %(message)s")

    if not args.input.exists():
        raise FileNotFoundError(f"GeoIP database not found: {args.input}")

    workers = args.workers or max(1, mp.cpu_count())
    client = connect(args.host)
    try:
        collection = client.ip_data.dns
        total = collection.count_documents(build_query())
    finally:
        client.close()

    if total == 0:
        log.info("Nothing to do; no documents missing GeoIP data")
        return

    log.info(
        "Starting GeoIP enrichment | host=%s input=%s workers=%s total=%s",
        args.host,
        args.input,
        workers,
        total,
    )

    method = "fork" if platform.system() == "Linux" else "spawn"
    log.info("Using multiprocessing start method: %s", method)
    ctx = mp.get_context(method)

    with ctx.Pool(processes=workers) as pool:
        pool.starmap(
            worker,
            [(args.host, str(args.input), args.claim_timeout)
             for _ in range(workers)],
        )

    log.info("GeoIP enrichment complete")


if __name__ == "__main__":
    main()
