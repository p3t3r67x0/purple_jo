#!/usr/bin/env python3
"""
scan_with_cpe.py

1) Downloads official NVD CPE 2.3 dictionary (cached).
2) Builds an index mapping normalized vendor/product -> list of CPE entries.
3) Scans a target with nmap (optional) or accepts banner input.
4) Maps banner (product+version) -> candidate CPEs using name normalization and version checks.
5) Looks up matched CVEs in MongoDB (if available).

Usage:
    # build/download CPE index (first run will download ~MBs)
    python scan_with_cpe.py --build-index

    # scan a target and lookup CVEs
    python scan_with_cpe.py scan example.com --ports 1-1024

    # map a banner string directly
    python scan_with_cpe.py map "nginx" "1.18.0"
"""
import tarfile
import gzip
import json
import re
import argparse
from pathlib import Path
from urllib.parse import urljoin

import requests
from tqdm import tqdm
from packaging.version import Version, InvalidVersion
from rapidfuzz import fuzz, process

# optional: for scanning
try:
    import nmap
except Exception:
    nmap = None

# mongodb
from pymongo import MongoClient

# ----------------------
# Config
# ----------------------
CACHE_DIR = Path.home() / ".cache" / "nvd"
CPE_TAR_GZ = CACHE_DIR / "nvdcpe-2.0.tar.gz"
CPE_INDEX_JSON = CACHE_DIR / "nvd_cpe_index.json"
NVD_CPE_URL = "https://nvd.nist.gov/feeds/json/cpe/2.0/nvdcpe-2.0.tar.gz"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "nvd"
CPES_COLLECTION = "cve_cpes"
CVES_COLLECTION = "cves"

# tuning
NAME_SIMILARITY_THRESHOLD = 80  # rapidfuzz percent (0-100) for fuzzy match
MAX_CANDIDATES_PER_NAME = 200   # cap to avoid huge candidate lists
GENERIC_TOKENS = {"httpd", "server", "service", "daemon"}


# ----------------------
# Utilities
# ----------------------
def ensure_cache_dir():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def download_cpe_dictionary(force=False):
    ensure_cache_dir()
    if CPE_TAR_GZ.exists() and not force:
        print(f"[+] Using cached CPE tar.gz: {CPE_TAR_GZ}")
        return CPE_TAR_GZ
    print(f"[+] Downloading CPE tar.gz (~60MB)...")
    resp = requests.get(NVD_CPE_URL, stream=True, timeout=60)
    resp.raise_for_status()
    with open(CPE_TAR_GZ, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"[+] Saved to {CPE_TAR_GZ}")
    return CPE_TAR_GZ


def clean_version(v: str) -> str:
    if not v:
        return ""
    v = v.lower()
    # strip distro cruft
    v = re.sub(r"\b(ubuntu|debian|centos|redhat|suse)[^ ]*", "", v)
    # OpenSSH style: 9.6p1 -> 9.6
    v = re.sub(r"(\d+\.\d+)p\d+", r"\1", v)
    # take only the first token
    v = v.strip().split()[0]
    return v


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"[^a-z0-9]+", " ", name.lower())
    tokens = [t for t in s.split() if t and t not in GENERIC_TOKENS]
    return " ".join(tokens)


def parse_cpe23_uri(cpe23: str):
    """
    Parse cpe:2.3:a:vendor:product:version:... and return dict fields.
    Very small parser for extracting vendor/product/version.
    """
    # cpe:2.3:part:vendor:product:version:update:edition:language:sw_edition:target_sw:target_hw:other
    parts = cpe23.split(":")
    if len(parts) < 6:
        return None
    return {
        "part": parts[2],
        "vendor": parts[3],
        "product": parts[4],
        "version": parts[5],
        "full": cpe23,
    }


# ----------------------
# Build index
# ----------------------


def build_cpe_index(force_download=False, force_reindex=False):
    ensure_cache_dir()
    if CPE_INDEX_JSON.exists() and not force_reindex:
        print(f"[+] Loading existing CPE index: {CPE_INDEX_JSON}")
        with open(CPE_INDEX_JSON, "r", encoding="utf-8") as fh:
            return json.load(fh)

    gzpath = download_cpe_dictionary(force=force_download)

    print("[+] Parsing CPE dictionary and building index (this will take a while)...")

    items = []
    with tarfile.open(gzpath, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".json"):
                continue
            f = tar.extractfile(member)
            if not f:
                continue
            payload = json.load(f)  # plain JSON, no gzip
            if "cpeItems" in payload:  # legacy feed
                items.extend(payload["cpeItems"])
            elif "products" in payload:  # new CPE 2.0 chunks
                if isinstance(payload["products"], list):
                    for prod in payload["products"]:
                        cpe23 = None
                        if "cpe" in prod:
                            cpe23 = prod["cpe"].get("cpeName")
                        elif "cpe23Uri" in prod:
                            cpe23 = prod["cpe23Uri"]
                        if cpe23:
                            items.append({"cpe23Uri": cpe23})
                # old nested dict structure
                elif isinstance(payload["products"], dict):
                    for vendor, vdata in payload["products"].items():
                        for product, pdata in vdata.get("product", {}).items():
                            for ver_entry in pdata.get("version", {}).values():
                                for cpe23 in ver_entry.get("cpe23Uri", []):
                                    items.append({"cpe23Uri": cpe23})

    # now proceed with your existing indexing logic using `items`
    index = {}
    seen = 0
    for it in tqdm(items, desc="Indexing CPEs"):
        cpe23 = it.get("cpe23Uri") or (
            it.get("cpe", {}).get("cpe23Uri") if "cpe" in it else None
        )
        if not cpe23:
            continue
        parsed = parse_cpe23_uri(cpe23)
        if not parsed:
            continue
        vendor_norm = normalize_name(parsed["vendor"])
        product_norm = normalize_name(parsed["product"])
        keys = set()
        if vendor_norm:
            keys.add(vendor_norm)
        if product_norm:
            keys.add(product_norm)
        if vendor_norm and product_norm:
            keys.add(f"{vendor_norm} {product_norm}")
            keys.add(f"{product_norm} {vendor_norm}")

        entry = {
            "vendor": parsed["vendor"],
            "product": parsed["product"],
            "version": parsed["version"],
            "cpe": parsed["full"]
        }
        for k in keys:
            index.setdefault(k, []).append(entry)
        seen += 1

    for k, arr in list(index.items()):
        if len(arr) > MAX_CANDIDATES_PER_NAME:
            index[k] = arr[:MAX_CANDIDATES_PER_NAME]

    with open(CPE_INDEX_JSON, "w", encoding="utf-8") as fh:
        json.dump(index, fh)

    print(f"[+] Index built with ~{seen} CPEs, saved to {CPE_INDEX_JSON}")
    return index


# ----------------------
# Matching logic
# ----------------------
def candidate_cpes_for_banner(index, product: str, version: str, vendor_hint: str = None):
    """
    Given a product name and version, return a list of candidate CPE dicts sorted by score.
    We try several strategies:
      - exact normalized product/vendor lookup
      - vendor+product lookup
      - fuzzy matches using rapidfuzz
    """
    prod_norm = normalize_name(product)
    vendor_norm = normalize_name(vendor_hint) if vendor_hint else None

    candidates = []
    tried_keys = set()

    # direct product key
    if prod_norm in index:
        tried_keys.add(prod_norm)
        candidates.extend(index[prod_norm])

    # vendor+product
    if vendor_norm:
        k = f"{vendor_norm} {prod_norm}"
        if k in index and k not in tried_keys:
            tried_keys.add(k)
            candidates.extend(index[k])

    # product+vendor
    if vendor_norm:
        k2 = f"{prod_norm} {vendor_norm}"
        if k2 in index and k2 not in tried_keys:
            tried_keys.add(k2)
            candidates.extend(index[k2])

    # vendor only
    if vendor_norm and vendor_norm in index and vendor_norm not in tried_keys:
        tried_keys.add(vendor_norm)
        candidates.extend(index[vendor_norm])

    # if few candidates, try fuzzy matching on index keys
    if len(candidates) < 20:
        choices = list(index.keys())
        matches = process.extract(
            prod_norm, choices, scorer=fuzz.token_sort_ratio, limit=10
        )
        for key, score, _ in matches:
            if score >= NAME_SIMILARITY_THRESHOLD:
                candidates.extend(index.get(key, []))

    # dedupe by cpe
    seen = set()
    deduped = []
    for c in candidates:
        if c["cpe"] in seen:
            continue
        seen.add(c["cpe"])
        deduped.append(c)

    # if version not provided, bias toward latest versions
    if not version:
        try:
            deduped.sort(key=lambda c: Version(c["version"]), reverse=True)
        except Exception:
            pass

    # Score candidates
    scored = []
    for c in deduped:
        name_score = fuzz.token_sort_ratio(
            prod_norm, normalize_name(c["product"]))
        ver_score = version_compatibility_score(c["version"], version)
        vendor_score = 1.0 if vendor_norm and vendor_norm == normalize_name(
            c["vendor"]) else 0.0
        # combined score: name > version > vendor
        combined = (0.6 * name_score) + (0.3 * (ver_score * 100)) + \
            (0.1 * (vendor_score * 100))
        scored.append((combined, name_score, ver_score, c))

    scored.sort(reverse=True, key=lambda t: t[0])
    return scored[:50]


def version_compatibility_score(cpe_version: str, detected_version: str):
    """
    Compare version strings and return score [0..1].
    """
    if not cpe_version or cpe_version in ("*", "-", "NA", "n/a"):
        return 0.5
    if not detected_version:
        return 0.0

    # strip OpenSSH-style "p1" suffixes
    cpe_version = re.sub(r"p\d+$", "", cpe_version)
    detected_version = re.sub(r"p\d+$", "", detected_version)

    try:
        cv = Version(cpe_version)
        dv = Version(detected_version)
        if cv == dv:
            return 1.0
        if cv.major == dv.major and cv.minor == dv.minor:
            return 0.9
        if cv.major == dv.major:
            return 0.7
        return 0.0
    except InvalidVersion:
        if cpe_version.startswith(detected_version) or detected_version.startswith(cpe_version):
            return 0.8
        if cpe_version in detected_version or detected_version in cpe_version:
            return 0.7
        return fuzz.partial_ratio(cpe_version, detected_version) / 100.0


# ----------------------
# Mongo lookup
# ----------------------
def lookup_cves_for_candidates(candidates, mongo_uri=MONGO_URI):
    """
    Given candidates as returned by candidate_cpes_for_banner (list of (score,name_score,ver_score,c)),
    query MongoDB cve_cpes -> cves and return matched CVE list with metadata.
    """
    client = MongoClient(mongo_uri)
    db = client[DB_NAME]
    cpes_col = db[CPES_COLLECTION]
    cves_col = db[CVES_COLLECTION]

    results = []
    for combined, name_score, ver_score, c in candidates:
        cpe_uri = c["cpe"]
        # look up cve_cpes entries for this cpe URI
        for match in cpes_col.find({"cpe23Uri": cpe_uri}):
            cve_doc = cves_col.find_one({"cve_id": match["cve_id"]})
            if not cve_doc:
                continue
            results.append({
                "cpe": cpe_uri,
                "vendor": c["vendor"],
                "product": c["product"],
                "detected_version_score": ver_score,
                "name_score": name_score,
                "combined_score": combined,
                "cve_id": cve_doc["cve_id"],
                "cvss": cve_doc.get("cvss_v3"),
                "description": cve_doc.get("description")
            })
    # sort by combined_score then cvss
    results.sort(key=lambda x: (
        x["combined_score"], (x["cvss"] or {}).get("baseScore", 0)), reverse=True)
    return results


# ----------------------
# Scanning helpers
# ----------------------
def run_nmap_scan(target, ports="1-1024"):
    """
    Run nmap -sV to fetch service/product/version. Requires system nmap installed.
    Returns list of dicts: {port, service, product, version}
    """
    if not nmap:
        raise RuntimeError("python-nmap not installed or failed to import.")
    nm = nmap.PortScanner()
    # -sV for version detection; --version-intensity 0..9 (lower faster)
    nm.scan(target, ports, arguments="-sV --version-intensity 2")
    results = []
    for host in nm.all_hosts():
        for proto in nm[host].all_protocols():
            lports = sorted(nm[host][proto].keys())
            for port in lports:
                info = nm[host][proto][port]
                if info.get("state") != "open":
                    continue
                product = info.get("product") or ""
                version = info.get("version") or ""
                name = info.get("name") or ""
                results.append({"port": port, "service": name,
                               "product": product, "version": version})
    return results


# ----------------------
# CLI wiring
# ----------------------
def main():
    parser = argparse.ArgumentParser(
        description="Map service banners to official NVD CPEs and CVEs.")
    sub = parser.add_subparsers(dest="cmd")

    sub_build = sub.add_parser(
        "build-index", help="Download & build the CPE index (cached).")
    sub_build.add_argument("--force-download", action="store_true")
    sub_build.add_argument("--force-reindex", action="store_true")

    sub_map = sub.add_parser(
        "map", help="Map an individual banner (product + version) to CPEs.")
    sub_map.add_argument("product")
    sub_map.add_argument("version", nargs="?")
    sub_map.add_argument("--vendor", help="Optional vendor hint")

    sub_scan = sub.add_parser(
        "scan", help="Run nmap on target and map results to CPEs + CVEs.")
    sub_scan.add_argument("target")
    sub_scan.add_argument("--ports", default="1-1024")
    sub_scan.add_argument("--mongo", action="store_true",
                          help="Lookup CVEs in MongoDB (requires running DB)")

    args = parser.parse_args()

    if args.cmd == "build-index":
        build_cpe_index(force_download=args.force_download,
                        force_reindex=args.force_reindex)
        return

    # load index (build if missing)
    index = build_cpe_index()

    if args.cmd == "map":
        product = args.product
        version = args.version or ""
        vendor = args.vendor
        candidates = candidate_cpes_for_banner(
            index, product, version, vendor_hint=vendor)
        print(f"[+] Found {len(candidates)} candidate CPEs (top 10):")
        for combined, name_score, ver_score, c in candidates[:10]:
            print(
                f"  CPE: {c['cpe']}  product={c['product']} vendor={c['vendor']}  name_score={name_score} ver_score={ver_score:.2f} combined={combined:.1f}")
        if args.vendor:
            print("[i] Vendor hint provided:", args.vendor)
        return

    if args.cmd == "scan":
        if not nmap:
            print(
                "[-] python-nmap not installed; cannot run scan. Install python-nmap and system nmap.")
            return
        print(f"[+] Scanning target {args.target} ports {args.ports} ...")
        services = run_nmap_scan(args.target, args.ports)
        if not services:
            print("[-] No open services found / no results from nmap.")
            return
        for svc in services:
            product = svc.get("product") or svc.get("service")
            version = clean_version(svc.get("version") or "")
            print(
                f"\n[service] port={svc['port']} product='{product}' version='{version}'")
            candidates = candidate_cpes_for_banner(index, product, version)
            if not candidates:
                print("  [-] No candidate CPEs found.")
                continue
            # show top 3
            for combined, name_score, ver_score, c in candidates[:5]:
                print(
                    f"  candidate CPE: {c['cpe']}  product={c['product']} vendor={c['vendor']}  combined={combined:.1f} name={name_score} ver={ver_score:.2f}")

            if args.mongo:
                matches = lookup_cves_for_candidates(candidates)
                if matches:
                    print("  -> matched CVEs (top 10):")
                    for m in matches[:10]:
                        score = m.get("cvss", {}).get("baseScore")
                        sev = m.get("cvss", {}).get("baseSeverity")
                        print(
                            f"    {m['cve_id']} | score={score} sev={sev} | {m['description'][:120]}...")
                else:
                    print("  -> no CVEs found in MongoDB for these candidates.")

        return

    parser.print_help()


if __name__ == "__main__":
    main()
