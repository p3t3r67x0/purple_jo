#!/usr/bin/env python3

import multiprocessing
import click
import pyasn
from datetime import datetime
from pymongo import MongoClient

# Global ASN DB (loaded once in main)
asn_db = None


def connect(host):
    """Return a new MongoDB client for this process."""
    return MongoClient(f"mongodb://{host}:27017")


def retrieve_ips(db, skip, limit):
    """Retrieve IP records with pagination."""
    cursor = db.ipv4.find({"name": {"$exists": False}}).skip(skip).limit(limit)
    return list(cursor)


def asn_lookup(ipv4):
    """Lookup ASN info from global pyasn db."""
    global asn_db
    asn, prefix = asn_db.lookup(ipv4)
    name = asn_db.get_as_name(asn)
    return {"name": name, "asn": asn, "prefix": prefix}


def worker_func(args):
    """Worker task: update ASN info for a batch of IPs."""
    host, skip, limit = args
    client = connect(host)
    db = client.ip_data

    ips = retrieve_ips(db, skip, limit)
    print(
        f"[Worker {multiprocessing.current_process().name}] Loaded {len(ips)} IPs (skip={skip}, limit={limit})"
    )

    for doc in ips:
        ip = doc["ip"]
        print(
            f"[Worker {multiprocessing.current_process().name}] Processing {ip}"
        )
        res = asn_lookup(ip)

        db.ipv4.update_one(
            {"ip": ip},
            {"$set": {"updated": datetime.now(), **res}},
            upsert=False,
        )

        print(
            f"[Worker {multiprocessing.current_process().name}] Updated {ip} with ASN {res['name']}"
        )

    client.close()


@click.command()
@click.option("--worker", required=True, type=int, help="number of worker processes")
@click.option("--host", required=True, type=str, help="MongoDB host")
def main(worker, host):
    global asn_db
    asn_db = pyasn.pyasn(
        "../../data/rib.20250901.1600.dat",
        as_names_file="../../data/asn_names.json",
    )

    client = connect(host)
    db = client.ip_data
    total = db.ipv4.count_documents({"name": {"$exists": False}})
    client.close()

    # Split work evenly across workers
    batch_size = (total + worker - 1) // worker
    tasks = [(host, i * batch_size, batch_size) for i in range(worker)]

    print(
        f"INFO: Found {total} IPs, batch size {batch_size}, spawning {worker} workers"
    )

    with multiprocessing.Pool(processes=worker) as pool:
        pool.map(worker_func, tasks)


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")  # workers inherit ASN DB
    main()
