#!/usr/bin/env python3
import argparse
import multiprocessing

from geoip2 import database
from geoip2.errors import AddressNotFoundError

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, CursorNotFound


def connect(host: str):
    return MongoClient(f"mongodb://{host}:27017").ip_data


def retrieve_domains(db, skip: int, limit: int):
    """Retrieve domains that have an A record but no country_code yet."""
    cursor = db.dns.find(
        {"a_record.0": {"$exists": True}, "country_code": {"$exists": False}},
        {"a_record": 1},
        skip=skip,
        limit=limit,
    )
    return list(cursor)


def update_data(db, ip: str, post: dict):
    """Update one DNS document by IP."""
    try:
        res = db.dns.update_one(
            {"a_record": {"$in": [ip]}},
            {"$set": post},
            upsert=False,
        )
        if res.modified_count > 0:
            print(
                f"[INFO] updated {ip} country_code={post.get('country_code')} "
                f"(docs={res.modified_count})"
            )
    except DuplicateKeyError:
        pass


def extract_geodata(db, reader, ip: str):
    """Extract GeoIP data and update Mongo."""
    try:
        response = reader.city(ip)
        post = {
            "country_code": response.country.iso_code,
            "country": response.country.name,
            "state": response.subdivisions.most_specific.name,
            "city": response.city.name,
            "loc": {
                "coordinates": [
                    round(response.location.longitude, 5)
                    if response.location.longitude is not None
                    else None,
                    round(response.location.latitude, 5)
                    if response.location.latitude is not None
                    else None,
                ]
            },
        }
        update_data(db, ip, post)
    except AddressNotFoundError:
        print(f"[WARN] IP not found in GeoIP database: {ip}")


def worker(host: str, input_file: str, skip: int, limit: int):
    """Worker function run in each process."""
    client = MongoClient(f"mongodb://{host}:27017")
    db = client.ip_data
    reader = database.Reader(input_file)

    try:
        domains = retrieve_domains(db, skip, limit)
        for domain in domains:
            for ip in domain.get("a_record", []):
                extract_geodata(db, reader, ip)
    except CursorNotFound:
        pass
    finally:
        reader.close()
        client.close()


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, required=True,
                        help="number of processes")
    parser.add_argument("--input", type=str, required=True,
                        help="GeoIP2 DB file")
    parser.add_argument("--host", type=str, required=True, help="MongoDB host")
    return parser.parse_args()


if __name__ == "__main__":
    args = argparser()
    client = MongoClient(f"mongodb://{args.host}:27017")
    db = client.ip_data
    total = db.dns.estimated_document_count()
    client.close()

    # divide into ranges
    batch = max(1, total // args.workers)
    ranges = [(i * batch, batch) for i in range(args.workers)]

    print(f"[INFO] Spawning {args.workers} workers, batch size={batch}")

    with multiprocessing.Pool(processes=args.workers) as pool:
        pool.starmap(worker, [(args.host, args.input, skip, limit)
                     for skip, limit in ranges])

    print("[INFO] All workers finished")
