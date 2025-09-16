#!/usr/bin/env python3
import argparse
import ipaddress
import multiprocessing
from datetime import datetime

from ipwhois.net import Net
from ipwhois.asn import ASNOrigin, IPASN
from ipwhois.exceptions import ASNOriginLookupError
from ipaddress import AddressValueError

from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError, DocumentTooLarge


BATCH_SIZE = 100  # how many docs each worker processes at once


def connect(host):
    return MongoClient(f"mongodb://{host}:27017", connect=False)


def get_whois(ip: str):
    try:
        return IPASN(Net(ip)).lookup(retry_count=0, asn_methods=["whois"])
    except Exception:
        return None


def get_cidr(ip: str, asn: str):
    try:
        cidrs = []
        result = ASNOrigin(Net(ip)).lookup(
            asn=str(asn), retry_count=10, asn_methods=["whois"]
        )
        if result and "nets" in result and result["nets"]:
            for c in result["nets"]:
                cidrs.append(c["cidr"])
        return cidrs
    except ASNOriginLookupError:
        return []


def handle_dns_batch(db, docs):
    ops = []
    for dns in docs:
        ip = dns["a_record"][0]
        whois = get_whois(ip)
        print(f"INFO: fetched {whois} for {ip}")
        if not whois:
            continue
        try:
            asn_cidr = whois["asn_cidr"]
            if ipaddress.IPv4Address(ip) in ipaddress.IPv4Network(asn_cidr):
                ops.append(
                    UpdateOne(
                        {"a_record.0": ip},
                        {"$set": {"updated": datetime.now(), "whois": whois}},
                        upsert=False,
                    )
                )
        except (AddressValueError, DuplicateKeyError):
            continue
    if ops:
        db.dns.bulk_write(ops, ordered=False)


def handle_ipv4_batch(db, docs):
    ops = []
    for asn in docs:
        ip = asn["ip"]
        whois = get_whois(ip)
        if not whois:
            continue
        try:
            ops.append(
                UpdateOne(
                    {"asn": ip},
                    {
                        "$set": {"updated": datetime.now(), "whois": whois},
                        "$unset": {"subnet": 0},
                    },
                    upsert=False,
                )
            )
        except (DocumentTooLarge, DuplicateKeyError):
            continue
    if ops:
        db.lookup.bulk_write(ops, ordered=False)


def worker(host, collection):
    client = connect(host)
    db = client.ip_data

    while True:
        query = {"whois.asn": {"$exists": False}}
        if collection == "dns":
            query["a_record.0"] = {"$exists": True}

        docs = list(db[collection].find(query, limit=BATCH_SIZE))
        if not docs:
            break

        ids = [d["_id"] for d in docs]
        db[collection].update_many({"_id": {"$in": ids}}, {
                                   "$set": {"claimed": True}})

        if collection == "dns":
            handle_dns_batch(db, docs)
        else:
            handle_ipv4_batch(db, docs)

    client.close()


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", choices=["dns", "ipv4"], required=True)
    parser.add_argument("--worker", type=int, required=True)
    parser.add_argument("--host", type=str, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = argparser()

    print(
        f"INFO: starting WHOIS fetcher for collection={args.collection}, workers={args.worker}, batch_size={BATCH_SIZE}"
    )

    with multiprocessing.Pool(processes=args.worker) as pool:
        pool.starmap(worker, [(args.host, args.collection)] * args.worker)

    print("INFO: all workers finished.")
