#!/usr/bin/env python3
import argparse
from pymongo import MongoClient
from pprint import pprint

# ----------------------
# CONFIG
# ----------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "nvd"
CVES_COLLECTION = "cves"
CPES_COLLECTION = "cve_cpes"


def connect():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[CVES_COLLECTION], db[CPES_COLLECTION]


# ----------------------
# Queries
# ----------------------
def get_cve(cves_col, cve_id: str):
    doc = cves_col.find_one({"cve_id": cve_id})
    if not doc:
        print(f"[-] CVE {cve_id} not found.")
        return
    pprint(doc)


def search_description(cves_col, keyword: str, limit: int = 10):
    cursor = cves_col.find({"description": {"$regex": keyword, "$options": "i"}}).limit(limit)
    for doc in cursor:
        print(f"{doc['cve_id']}: {doc.get('description', '')[:100]}...")


def search_cpe(cpes_col, product: str, limit: int = 10):
    cursor = cpes_col.find({"cpe23Uri": {"$regex": product, "$options": "i"}}).limit(limit)
    for doc in cursor:
        print(f"{doc['cve_id']} -> {doc['cpe23Uri']}")


def list_high_severity(cves_col, min_score: float = 7.0, limit: int = 10):
    cursor = (
        cves_col.find({"cvss_v3.baseScore": {"$gte": min_score}})
        .sort("cvss_v3.baseScore", -1)
        .limit(limit)
    )
    for doc in cursor:
        print(f"{doc['cve_id']} (Score {doc['cvss_v3']['baseScore']}): {doc.get('description','')[:80]}...")


# ----------------------
# CLI
# ----------------------
def main():
    parser = argparse.ArgumentParser(description="Query NVD CVE MongoDB database")
    subparsers = parser.add_subparsers(dest="command")

    # cve by id
    p_id = subparsers.add_parser("get", help="Fetch CVE by ID")
    p_id.add_argument("cve_id")

    # search description
    p_desc = subparsers.add_parser("search", help="Search CVEs by keyword in description")
    p_desc.add_argument("keyword")
    p_desc.add_argument("--limit", type=int, default=10)

    # search CPEs
    p_cpe = subparsers.add_parser("cpe", help="Search CVEs by product/vendor keyword in CPE")
    p_cpe.add_argument("product")
    p_cpe.add_argument("--limit", type=int, default=10)

    # list high severity
    p_high = subparsers.add_parser("high", help="List high severity CVEs")
    p_high.add_argument("--min-score", type=float, default=7.0)
    p_high.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()

    cves_col, cpes_col = connect()

    if args.command == "get":
        get_cve(cves_col, args.cve_id)
    elif args.command == "search":
        search_description(cves_col, args.keyword, args.limit)
    elif args.command == "cpe":
        search_cpe(cpes_col, args.product, args.limit)
    elif args.command == "high":
        list_high_severity(cves_col, args.min_score, args.limit)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()