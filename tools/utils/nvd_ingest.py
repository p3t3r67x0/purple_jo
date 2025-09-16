#!/usr/bin/env python3

import requests
import gzip
import json
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential

# ----------------------
# CONFIG
# ----------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "nvd"
CVES_COLLECTION = "cves"
CPES_COLLECTION = "cve_cpes"

NVD_FEED_BASE = "https://nvd.nist.gov/feeds/json/cve/2.0/"
YEARS = list(range(2020, 2025))   # adjust as needed
INCLUDE_MODIFIED_FEED = True      # pull modified feed for incremental updates


# ----------------------
# Helpers
# ----------------------
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def fetch_and_parse_feed(url: str):
    """Download, decompress, and parse NVD JSON 2.0 feed with retries."""
    print(f"[+] Fetching {url}")
    resp = requests.get(url, timeout=120, headers={
                        "User-Agent": "NVD-Ingest/2.0"})
    resp.raise_for_status()
    data = gzip.decompress(resp.content).decode("utf-8")
    return json.loads(data)


def extract_cve_doc(cve: dict) -> dict:
    """Extract top-level CVE doc (pass in cve object, not wrapper)."""
    cve_id = cve["id"]

    # description
    description = None
    for d in cve.get("descriptions", []):
        if d.get("lang") == "en":
            description = d.get("value")

    # CVSS v3.1 if present
    cvss_v3 = None
    metrics = cve.get("metrics", {})
    if "cvssMetricV31" in metrics:
        metric = metrics["cvssMetricV31"][0]
        cvss_data = metric.get("cvssData", {})
        cvss_v3 = {
            "baseScore": cvss_data.get("baseScore"),
            "baseSeverity": cvss_data.get("baseSeverity"),
            "vectorString": cvss_data.get("vectorString"),
        }

    return {
        "_id": cve_id,
        "cve_id": cve_id,
        "description": description,
        "publishedDate": cve.get("published"),
        "lastModifiedDate": cve.get("lastModified"),
        "cvss_v3": cvss_v3,
    }


def extract_cpe_docs(cve: dict) -> list:
    """Extract affected CPEs for a CVE as separate docs."""
    cve_id = cve["id"]
    docs = []
    for node in cve.get("configurations", []):
        for n in node.get("nodes", []):
            for match in n.get("cpeMatch", []):
                docs.append({
                    "cve_id": cve_id,
                    "cpe23Uri": match.get("criteria"),
                    "vulnerable": match.get("vulnerable", False),
                    "versionStartIncluding": match.get("versionStartIncluding"),
                    "versionStartExcluding": match.get("versionStartExcluding"),
                    "versionEndIncluding": match.get("versionEndIncluding"),
                    "versionEndExcluding": match.get("versionEndExcluding"),
                })
    return docs


def bulk_upsert(collection, docs, key_field="_id"):
    """Bulk upsert list of docs into MongoDB."""
    if not docs:
        return
    ops = []
    for doc in docs:
        ops.append(UpdateOne(
            {key_field: doc[key_field]},
            {"$set": doc},
            upsert=True
        ))
    result = collection.bulk_write(ops, ordered=False)
    print(
        f"    Inserted: {result.upserted_count}, "
        f"Modified: {result.modified_count}"
    )


# ----------------------
# Main
# ----------------------
def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    cves_col = db[CVES_COLLECTION]
    cpes_col = db[CPES_COLLECTION]

    feeds = [f"{NVD_FEED_BASE}nvdcve-2.0-{year}.json.gz" for year in YEARS]
    if INCLUDE_MODIFIED_FEED:
        feeds.append(f"{NVD_FEED_BASE}nvdcve-2.0-modified.json.gz")

    for url in feeds:
        try:
            feed = fetch_and_parse_feed(url)
        except requests.exceptions.HTTPError as e:
            print(f"[-] Error fetching {url}: {e}")
            continue

        items = feed.get("vulnerabilities", [])
        print(f"[+] Processing {len(items)} CVEs from {url}")

        cve_batch, cpe_batch = [], []
        for wrapper in tqdm(items, desc=" Normalizing"):
            cve = wrapper["cve"]              # unwrap once
            cve_doc = extract_cve_doc(cve)    # pass the CVE dict directly
            cve_batch.append(cve_doc)

            cpe_docs = extract_cpe_docs(cve)
            for cpe in cpe_docs:
                # use compound key to avoid dupes: (cve_id + cpe23Uri)
                cpe["_id"] = f"{cpe['cve_id']}::{cpe['cpe23Uri']}"
                cpe_batch.append(cpe)

            # flush in chunks
            if len(cve_batch) >= 1000:
                bulk_upsert(cves_col, cve_batch, key_field="_id")
                cve_batch = []
            if len(cpe_batch) >= 2000:
                bulk_upsert(cpes_col, cpe_batch, key_field="_id")
                cpe_batch = []

        # flush leftovers
        if cve_batch:
            bulk_upsert(cves_col, cve_batch, key_field="_id")
        if cpe_batch:
            bulk_upsert(cpes_col, cpe_batch, key_field="_id")

    print("[✓] Ingestion complete.")


if __name__ == "__main__":
    main()
