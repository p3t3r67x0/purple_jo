#!/usr/bin/env python3
import sys
import json
import argparse
import contextlib
import multiprocessing
import os
import subprocess
import tempfile
import time
from datetime import datetime

from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError, AutoReconnect


def load_ports(filename):
    try:
        with open(filename, "r") as f:
            ports = [line.strip() for line in f if line.strip()]
        return ",".join(ports)
    except IOError as e:
        print(f"ERROR: cannot read ports file {filename}: {e}")
        sys.exit(1)


def connect(host):
    return MongoClient(f"mongodb://{host}:27017", connect=False)


def claim_ips(db, batch_size):
    """Atomically claim a batch of IPs for scanning."""
    docs = list(
        db.dns.find(
            {"ports": {"$exists": False}, "claimed_port": {"$ne": True}},  # exclude already claimed_port
            {"a_record": 1}
        ).limit(batch_size)
    )
    if not docs:
        return []

    ids = [d["_id"] for d in docs]
    db.dns.update_many({"_id": {"$in": ids}}, {"$set": {"claimed_port": True}})
    ips = []
    for doc in docs:
        ips.extend(doc.get("a_record", []))
    return ips


def run_masscan(ips, ports, rate=1000):
    """Run masscan and return parsed results."""
    if not ips:
        return []

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        out_file = tmpfile.name

    cmd = [
        "masscan",
        "-p", ports,
        "--rate", str(rate),
        "--open",
        "--banners",
        "-oJ", out_file,
    ] + ips

    print(f"[INFO] Running: {' '.join(cmd)}")

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[-] masscan failed: {e}")
        return []

    try:
        with open(out_file, "r") as f:
            content = f.read()
    except Exception as e:
        print(f"[-] Failed to read masscan output: {e}")
        return []
    finally:
        with contextlib.suppress(Exception):
            os.remove(out_file)

    if not content.strip():
        return []

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        # Fallback to line-by-line JSON (masscan sometimes emits NDJSON style)
        results = []
        for raw in content.splitlines():
            line = raw.strip().rstrip(",")
            if not line or line in {"[", "]"}:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as e:
                preview = line[:80] + ("..." if len(line) > 80 else "")
                print(f"[-] Failed to parse masscan JSON line: {e} -> {preview}")
                continue

            if "ip" in payload and payload.get("ports"):
                results.append(payload)
            elif "results" in payload:
                results.extend(payload.get("results", []))
        return results

    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict) and "results" in parsed:
        return parsed.get("results", [])
    return []


def update_data(db, scan_results, now):
    """Bulk update lookup + dns collections with scan results."""
    lookup_ops = []
    dns_ops = []

    for result in scan_results:
        ip = result.get("ip")
        ports_info = result.get("ports", [])
        if not ip or not ports_info:
            continue

        port_data_list = [
            {
                "port": r.get("port"),
                "proto": r.get("proto", "tcp"),
                "status": r.get("status", "open"),
                "reason": r.get("reason", ""),
            }
            for r in ports_info
        ]

        lookup_ops.append(
            UpdateOne(
                {"ip": ip},
                {
                    "$set": {"updated": now},
                    "$addToSet": {"ports": {"$each": port_data_list}},
                },
                upsert=True,
            )
        )

        dns_ops.append(
            UpdateOne(
                {"a_record": {"$in": [ip]}},   # safer than "a_record": ip
                {"$set": {"updated": now, "ports": port_data_list}},
                upsert=False,
            )
        )

    if lookup_ops:
        try:
            res = db.lookup.bulk_write(lookup_ops, ordered=False)
            print(
                f"[INFO] bulk updated lookup: {res.modified_count} modified, {res.upserted_count} inserted")
        except (AutoReconnect, DuplicateKeyError):
            time.sleep(10)

    if dns_ops:
        try:
            res = db.dns.bulk_write(dns_ops, ordered=False)
            print(f"[INFO] bulk updated dns: {res.modified_count} modified")
        except (AutoReconnect, DuplicateKeyError):
            time.sleep(10)


def worker(host, ports, rate, batch_size):
    client = connect(host)
    db = client.ip_data

    while True:
        ips = claim_ips(db, batch_size)
        if not ips:
            break

        scan_results = run_masscan(ips, ports, rate=rate)
        update_data(db, scan_results, datetime.utcnow())

    client.close()


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, required=True,
                        help="number of processes")
    parser.add_argument("--input", type=str, required=True, help="ports file")
    parser.add_argument("--host", type=str, required=True, help="mongo host")
    parser.add_argument("--batch", type=int, default=500,
                        help="number of IPs per worker batch")
    parser.add_argument("--rate", type=int, default=5000, help="masscan rate")
    return parser.parse_args()


if __name__ == "__main__":
    args = argparser()
    ports = load_ports(args.input)

    print(f"[INFO] Ports loaded: {ports}")
    print(
        f"[INFO] Starting {args.workers} processes, batch size {args.batch}, rate {args.rate}")

    with multiprocessing.Pool(processes=args.workers) as pool:
        pool.starmap(
            worker, [(args.host, ports, args.rate, args.batch)] * args.workers)

    print("[INFO] All workers finished")
