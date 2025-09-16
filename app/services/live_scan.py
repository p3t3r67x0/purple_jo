# app/services/live_scan.py
from datetime import datetime

from .dns import fetch_dns_records
from .http import fetch_site_headers
from .ssl import extract_certificate
from .banners import grab_banner
from .masscan import run_masscan
from .whois import fetch_asn_whois
from .geoip import fetch_geoip
from .qrcode import generate_qrcode


async def perform_live_scan(mongo, domain: str) -> dict:
    """
    Run a full live scan enrichment pipeline for a domain.
    Includes DNS, headers, SSL, GeoIP, WHOIS, banners, ports, QR code.
    Saves the result to Mongo.
    """
    now = datetime.now().isoformat()
    dns_records = await fetch_dns_records(domain)
    headers = await fetch_site_headers(domain)
    ssl_info = await extract_certificate(domain)

    banner_info = None
    ports_info = []
    whois_info = {}
    geo_info = {}
    qrcode_info = generate_qrcode(domain=domain)

    if dns_records.get("a_record"):
        target_ip = dns_records["a_record"][0]

        banner_info = await grab_banner(target_ip)
        ports_info = await run_masscan(target_ip)
        whois_info = await fetch_asn_whois(target_ip)
        geo_info = await fetch_geoip(target_ip)

    live_result = {
        "domain": domain,
        "created": now,
        "updated": now,
        "a_record": dns_records.get("a_record"),
        "aaaa_record": dns_records.get("aaaa_record"),
        "ns_record": dns_records.get("ns_record"),
        "mx_record": dns_records.get("mx_record"),
        "soa_record": dns_records.get("soa_record"),
        "header": headers,
        "ports": ports_info,
        "whois": whois_info,
        "country_code": geo_info.get("country_code"),
        "city": geo_info.get("city"),
        "country": geo_info.get("country"),
        "state": geo_info.get("state"),
        "loc": geo_info.get("loc"),
        "qrcode": qrcode_info.get("qrcode"),
        "banner": banner_info,
        "ssl": ssl_info,
    }

    try:
        await mongo.dns.update_one(
            {"domain": domain},
            {"$set": live_result},
            upsert=True,
        )
    except Exception as e:
        print(f"MongoDB write error for {domain}: {e}")

    return live_result
