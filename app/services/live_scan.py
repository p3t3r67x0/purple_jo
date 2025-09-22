# app/services/live_scan.py
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional

import asyncio
import idna
from fastapi import HTTPException

from .dns import fetch_dns_records
from .http import fetch_site_headers
from .ssl import extract_certificate
from .banners import grab_banner
from .masscan import run_masscan
from .whois import fetch_asn_whois
from .geoip import fetch_geoip
from .qrcode import generate_qrcode


ProgressEvent = Dict[str, Any]
ProgressReporter = Callable[[ProgressEvent], Awaitable[None]]


async def _emit(reporter: Optional[ProgressReporter], event: ProgressEvent) -> None:
    if reporter:
        await reporter(event)


def _is_valid_domain(domain: str) -> bool:
    if not domain:
        return False

    try:
        ascii_domain = idna.encode(domain.strip()).decode("ascii")
    except idna.IDNAError:
        return False

    if ascii_domain.endswith("."):
        ascii_domain = ascii_domain[:-1]

    if len(ascii_domain) == 0 or len(ascii_domain) > 253:
        return False

    labels: List[str] = ascii_domain.split(".")
    if len(labels) < 2:
        return False

    for label in labels:
        if not 0 < len(label) <= 63:
            return False
        if label.startswith("-") or label.endswith("-"):
            return False

    return True


async def perform_live_scan(
    mongo,
    domain: str,
    reporter: Optional[ProgressReporter] = None,
) -> dict:
    """
    Run a full live scan enrichment pipeline for a domain.
    Includes DNS, headers, SSL, GeoIP, WHOIS, banners, ports, QR code.
    Saves the result to Mongo.
    """
    try:
        if not _is_valid_domain(domain):
            await _emit(
                reporter,
                {
                    "type": "error",
                    "message": "invalid_domain",
                    "domain": domain,
                },
            )
            raise HTTPException(status_code=400, detail={"error": "invalid_domain", "domain": domain})

        await _emit(
            reporter,
            {
                "type": "start",
                "domain": domain,
                "timestamp": datetime.now().isoformat(),
            },
        )

        now = datetime.now().isoformat()
        await _emit(reporter, {"type": "progress", "step": "dns", "status": "started"})
        dns_records = await fetch_dns_records(domain)
        await _emit(reporter, {"type": "progress", "step": "dns", "status": "completed"})

        await _emit(reporter, {"type": "progress", "step": "headers", "status": "started"})
        headers = await fetch_site_headers(domain)
        await _emit(reporter, {"type": "progress", "step": "headers", "status": "completed"})

        await _emit(reporter, {"type": "progress", "step": "ssl", "status": "started"})
        ssl_info = await extract_certificate(domain)
        await _emit(reporter, {"type": "progress", "step": "ssl", "status": "completed"})

        banner_info = None
        ports_info = []
        whois_info = {}
        geo_info = {}
        qrcode_info = generate_qrcode(domain=domain)

        if dns_records.get("a_record"):
            target_ip = dns_records["a_record"][0]

            await _emit(reporter, {"type": "progress", "step": "banner", "status": "started"})
            banner_info = await grab_banner(target_ip)
            await _emit(reporter, {"type": "progress", "step": "banner", "status": "completed"})

            await _emit(reporter, {"type": "progress", "step": "ports", "status": "started"})
            ports_info = await run_masscan(target_ip)
            await _emit(reporter, {"type": "progress", "step": "ports", "status": "completed"})

            await _emit(reporter, {"type": "progress", "step": "whois", "status": "started"})
            whois_info = await fetch_asn_whois(target_ip)
            await _emit(reporter, {"type": "progress", "step": "whois", "status": "completed"})

            await _emit(reporter, {"type": "progress", "step": "geoip", "status": "started"})
            geo_info = await fetch_geoip(target_ip)
            await _emit(reporter, {"type": "progress", "step": "geoip", "status": "completed"})

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
            "geo": geo_info,
            "banner": banner_info,
            "ssl": ssl_info,
        }

        try:
            await mongo.dns.update_one(
                {"domain": domain},
                {"$set": live_result},
                upsert=True,
            )
            await _emit(reporter, {"type": "progress", "step": "mongo", "status": "stored"})
        except Exception as e:
            await _emit(
                reporter,
                {
                    "type": "error",
                    "message": "mongo_write_failed",
                    "domain": domain,
                    "detail": str(e),
                },
            )
            print(f"MongoDB write error for {domain}: {e}")

        await _emit(reporter, {"type": "result", "data": live_result})
        return live_result

    except asyncio.CancelledError:
        await _emit(
            reporter,
            {
                "type": "error",
                "message": "scan_cancelled",
                "domain": domain,
                "detail": "Live scan was cancelled (client likely disconnected)",
            },
        )
        # Re-raise the CancelledError to properly handle the cancellation
        raise
