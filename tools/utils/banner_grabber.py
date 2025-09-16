#!/usr/bin/env python3

import socket
import multiprocessing
import argparse
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def connect(host):
    """Return a fresh MongoDB client for each process."""
    return MongoClient(f"mongodb://{host}:27017")


def update_data(db, doc_id, domain, ip, post):
    try:
        db.dns.update_one({"_id": doc_id}, {"$set": post}, upsert=False)
        print(f"INFO: updated domain {domain} with ip {ip} banner")
    except DuplicateKeyError:
        pass


def retrieve_batch(db, batch_size):
    """Atomically fetch a batch of documents to process and mark them as taken."""
    docs = list(
        db.dns.find(
            {
                "a_record": {"$exists": True},
                "banner": {"$exists": False},
                "ports.port": {"$in": [22]},
                "banner_scan_failed": {"$exists": False},
                "in_progress": {"$ne": True},
            },
            {"_id": 1, "domain": 1, "a_record": 1},
        )
        .sort([("updated", -1)])
        .limit(batch_size)
    )

    if docs:
        ids = [doc["_id"] for doc in docs]
        db.dns.update_many({"_id": {"$in": ids}}, {
                           "$set": {"in_progress": True}})

    return docs


def grab_banner(ip, port=22):
    """Try to grab a banner from IP:port."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((ip, port))
        banner = s.recv(1024)
        s.close()
        banner = banner.decode("utf-8", errors="ignore").strip()
        print(f"Grabbed banner from {ip}:{port} - {banner}")
        return banner
    except Exception as e:
        print(e)
        print(f"Failed to grab banner from {ip}:{port}")
        return None


def worker(host, batch_size):
    """Worker process: continuously fetch and process batches until empty."""
    client = connect(host)
    db = client.ip_data

    while True:
        docs = retrieve_batch(db, batch_size)
        if not docs:
            break

        print(
            f"[{multiprocessing.current_process().name}] Processing {len(docs)} docs")

        for document in docs:
            ip = document["a_record"][0]
            domain = document["domain"]
            banner = grab_banner(ip, 22)

            if banner:
                update_data(
                    db,
                    document["_id"],
                    domain,
                    ip,
                    {"banner": banner, "updated": datetime.now(),
                     "in_progress": False},
                )
            else:
                update_data(
                    db,
                    document["_id"],
                    domain,
                    ip,
                    {"banner_scan_failed": datetime.utcnow(), "in_progress": False},
                )

    client.close()


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", help="set worker count",
                        type=int, required=True)
    parser.add_argument("--host", help="set the host", type=str, required=True)
    parser.add_argument(
        "--batch", help="batch size per fetch", type=int, default=50, required=False
    )
    return parser.parse_args()


def main():
    args = argparser()

    print(
        f"INFO: Starting banner grabber with {args.worker} workers, batch size {args.batch}"
    )

    with multiprocessing.Pool(processes=args.worker) as pool:
        pool.starmap(worker, [(args.host, args.batch)] * args.worker)


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    main()
