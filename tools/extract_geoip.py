#!/usr/bin/env python3

"""Bulk GeoIP enrichment for DNS documents stored in MongoDB."""

from __future__ import annotations

import argparse
import logging
import math
import multiprocessing as mp
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from geoip2 import database
from geoip2.errors import AddressNotFoundError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import CursorNotFound, DuplicateKeyError

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


def retrieve_domains(collection: Collection, skip: int, limit: int) -> List[Dict[str, Any]]:
    """Fetch a window of candidate documents sorted by ``_id``."""

    try:
        cursor = collection.find(
            build_query(),
            {"a_record": 1},
            sort=[("_id", 1)],
            skip=skip,
            limit=limit,
        )
        return list(cursor)
    except CursorNotFound:
        log.warning("Cursor expired for slice skip=%s limit=%s", skip, limit)
        return []


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
                "$unset": {"geo_lookup_failed": ""},
            },
            upsert=False,
        )
        if res.modified_count > 0:
            log.info("Updated GeoIP for %s -> %s", ip, post.get("country_code"))
    except DuplicateKeyError:
        pass


def mark_geoip_failure(collection: Collection, ip: str) -> None:
    collection.update_one(
        {"a_record": {"$in": [ip]}},
        {"$set": {"geo_lookup_failed": datetime.now(timezone.utc)}},
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
        "loc": {"coordinates": _sanitize_coordinates(response.location.longitude, response.location.latitude)},
    }

    update_geo_fields(collection, ip, post)


def worker(host: str, input_file: str, skip: int, limit: int) -> None:
    process_name = mp.current_process().name
    log.info("Worker %s processing slice skip=%s limit=%s", process_name, skip, limit)

    client = connect(host)
    try:
        db: Database = client.ip_data
        collection = db.dns
        with database.Reader(input_file) as reader:
            domains = retrieve_domains(collection, skip, limit)
            log.info("Worker %s fetched %d domains", process_name, len(domains))
            for domain in domains:
                for ip in domain.get("a_record", []):
                    handle_ip(collection, reader, ip)
    finally:
        client.close()


def build_ranges(total: int, chunk_size: int) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    for start in range(0, total, chunk_size):
        ranges.append((start, min(chunk_size, total - start)))
    return ranges


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich Mongo DNS documents with GeoIP data")
    parser.add_argument("--input", required=True, type=Path,
                        help="Path to GeoIP2 City database (mmdb)")
    parser.add_argument("--host", default="localhost", type=str,
                        help="MongoDB host (default: localhost)")
    parser.add_argument("--workers", type=int, default=0,
                        help="Number of worker processes (0 = CPU count)")
    parser.add_argument("--chunk-size", type=int, default=0,
                        help="Documents per chunk (0 = auto based on workers)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

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

    chunk_size = args.chunk_size or max(1, math.ceil(total / workers))
    ranges = build_ranges(total, chunk_size)

    log.info(
        "Starting GeoIP enrichment | host=%s input=%s workers=%s total=%s chunk=%s",
        args.host,
        args.input,
        workers,
        total,
        chunk_size,
    )

    method = "fork" if platform.system() == "Linux" else "spawn"
    log.info("Using multiprocessing start method: %s", method)
    ctx = mp.get_context(method)

    with ctx.Pool(processes=workers) as pool:
        pool.starmap(worker, [(args.host, str(args.input), skip, limit)
                              for skip, limit in ranges])

    log.info("GeoIP enrichment complete")


if __name__ == "__main__":
    main()
