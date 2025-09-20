#!/usr/bin/env python3

import asyncio
import ssl
import re
import argparse
import multiprocessing
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne


BATCH_SIZE = 50      # how many domains each process pulls from DB at once
CONCURRENCY = 100    # concurrent TLS handshakes per process
TIMEOUT = 3          # socket/TLS timeout
TLS_PORTS = [443]    # ports to probe for TLS certificates


async def test_tls_version(domain: str, port: int = 443):
    """Try connecting with specific TLS versions and report which succeed."""
    results = {}

    # Map SSL module constants to human-readable labels
    tls_versions = {
        "TLSv1.0": ssl.TLSVersion.TLSv1,
        "TLSv1.1": ssl.TLSVersion.TLSv1_1,
        "TLSv1.2": ssl.TLSVersion.TLSv1_2,
        "TLSv1.3": ssl.TLSVersion.TLSv1_3,
    }

    for label, version in tls_versions.items():
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.minimum_version = version
            ctx.maximum_version = version
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    domain, port, ssl=ctx, server_hostname=domain),
                timeout=TIMEOUT,
            )
            ssl_obj = writer.get_extra_info("ssl_object")
            results[label] = {
                "accepted": True,
                "cipher": ssl_obj.cipher() if ssl_obj else None,
                "version": ssl_obj.version() if ssl_obj else None,
            }
            writer.close()
            await writer.wait_closed()
        except Exception:
            results[label] = {"accepted": False}

    return results


async def extract_certificate(domain: str):
    """Connect via TLS, extract cert fields + test protocol versions."""
    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(
                domain, 443, ssl=ssl_context, server_hostname=domain),
            timeout=TIMEOUT,
        )
        cert = writer.get_extra_info("peercert")
        ssl_obj = writer.get_extra_info("ssl_object")
        writer.close()
        await writer.wait_closed()
    except Exception:
        return None

    if not cert:
        return None

    issuer = {}
    subject = {}
    alt_names = []

    for item in cert.get("subject", []):
        subject["_".join(re.findall(".[^A-Z]*", item[0][0])
                         ).lower()] = item[0][1]

    for item in cert.get("issuer", []):
        issuer["_".join(re.findall(".[^A-Z]*", item[0][0])
                        ).lower()] = item[0][1]

    for item in cert.get("subjectAltName", []):
        alt_names.append(item[1])

    post = {
        "issuer": issuer,
        "subject": subject,
        "subject_alt_names": alt_names,
        "serial": cert.get("serialNumber"),
        "not_before": datetime.strptime(cert["notBefore"], "%b %d %H:%M:%S %Y %Z"),
        "not_after": datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z"),
        "version": cert.get("version"),
        "handshake_version": ssl_obj.version() if ssl_obj else None,
        "handshake_cipher": ssl_obj.cipher() if ssl_obj else None,
    }

    if "OCSP" in cert and cert["OCSP"]:
        post["ocsp"] = cert["OCSP"][0].strip("/")

    if "caIssuers" in cert and cert["caIssuers"]:
        post["ca_issuers"] = cert["caIssuers"][0].strip("/")

    if "crlDistributionPoints" in cert and cert["crlDistributionPoints"]:
        post["crl_distribution_points"] = cert["crlDistributionPoints"][0].strip(
            "/")

    post["tls_versions"] = await test_tls_version(domain)

    return post


async def handle_batch(db, docs):
    sem = asyncio.Semaphore(CONCURRENCY)
    now = datetime.now()

    async def bounded_task(domain):
        async with sem:
            cert = await extract_certificate(domain)
            print(cert)
            if cert:
                return UpdateOne({"domain": domain}, {"$set": {"ssl": cert, "updated": now}})
            else:
                return UpdateOne({"domain": domain}, {"$set": {"ssl_scan_failed": now}})

    tasks = [bounded_task(doc["domain"]) for doc in docs]
    ops = [op for op in await asyncio.gather(*tasks) if op]

    if ops:
        await db.dns.bulk_write(ops, ordered=False)


async def worker_loop(mongo_host):
    client = AsyncIOMotorClient(f"mongodb://{mongo_host}:27017")
    db = client.ip_data

    while True:
        docs = await db.dns.find(
            {
                "ssl": {"$exists": False},
                "cert_scan_failed": {"$exists": False},
                "ports": {
                    "$elemMatch": {
                        "port": {"$in": TLS_PORTS},
                        "status": "open",
                        "proto": "tcp",
                    }
                },
                "claimed_cert_scan": {"$exists": False},
            },
            {"domain": 1},
            limit=BATCH_SIZE,
        ).to_list(length=BATCH_SIZE)

        if not docs:
            break

        ids = [d["_id"] for d in docs]
        await db.dns.update_many({"_id": {"$in": ids}}, {"$set": {"claimed_cert_scan": True}})
        await handle_batch(db, docs)

    client.close()


def start_process(mongo_host):
    asyncio.run(worker_loop(mongo_host))


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, required=True, help="MongoDB host")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of processes")
    return parser.parse_args()


if __name__ == "__main__":
    args = argparser()
    print(
        f"[INFO] Starting {args.workers} processes, {CONCURRENCY} concurrent TLS scans each")

    with multiprocessing.Pool(processes=args.workers) as pool:
        pool.map(start_process, [args.host] * args.workers)

    print("[INFO] All workers finished")
