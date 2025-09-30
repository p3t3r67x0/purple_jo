"""Live scan pipeline that persists enrichment results into PostgreSQL."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

import idna
from fastapi import HTTPException
from sqlalchemy import delete, select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.postgres import (
    ARecord,
    AAAARecord,
    CNAMERecord,
    Domain,
    GeoPoint,
    MXRecord,
    NSRecord,
    PortService,
    SoaRecord,
    SSLData,
    SSLSubjectAltName,
    TXTRecord,
    WhoisRecord,
)

from .banners import grab_banner
from .dns import fetch_dns_records
from .geoip import fetch_geoip
from .http import fetch_site_headers
from .masscan import run_masscan
from .qrcode import generate_qrcode
from .ssl import extract_certificate
from .whois import fetch_asn_whois

ProgressEvent = Dict[str, Any]
ProgressReporter = Callable[[ProgressEvent], Awaitable[None]]

logger = logging.getLogger(__name__)


async def _emit(
    reporter: Optional[ProgressReporter], event: ProgressEvent
) -> None:
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
    domain: str,
    *,
    postgres_session: AsyncSession,
    reporter: Optional[ProgressReporter] = None,
) -> dict[str, Any]:
    """Run a full enrichment workflow for ``domain`` and persist into
    PostgreSQL.
    """

    if not _is_valid_domain(domain):
        await _emit(
            reporter,
            {
                "type": "error",
                "message": "invalid_domain",
                "domain": domain,
            },
        )
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_domain", "domain": domain},
        )

    now_dt = datetime.now(timezone.utc)
    await _emit(
        reporter,
        {
            "type": "start",
            "domain": domain,
            "timestamp": now_dt.isoformat(),
        },
    )

    # DNS
    await _emit(
        reporter, {"type": "progress", "step": "dns", "status": "started"}
    )
    dns_records = await fetch_dns_records(domain)
    await _emit(
        reporter, {"type": "progress", "step": "dns", "status": "completed"}
    )

    # HTTP headers
    await _emit(
        reporter, {"type": "progress", "step": "headers", "status": "started"}
    )
    headers = await fetch_site_headers(domain)
    await _emit(
        reporter,
        {"type": "progress", "step": "headers", "status": "completed"}
    )

    # SSL certificate
    await _emit(
        reporter, {"type": "progress", "step": "ssl", "status": "started"}
    )
    ssl_info = await extract_certificate(domain)
    await _emit(
        reporter, {"type": "progress", "step": "ssl", "status": "completed"}
    )

    banner_info = None
    ports_info: List[Dict[str, Any]] = []
    whois_info: Dict[str, Any] = {}
    geo_info: Dict[str, Any] = {}
    qrcode_info = generate_qrcode(domain=domain)

    if dns_records.get("a_record"):
        target_ip = dns_records["a_record"][0]

        await _emit(
            reporter,
            {"type": "progress", "step": "banner", "status": "started"}
        )
        banner_info = await grab_banner(target_ip)
        await _emit(
            reporter,
            {"type": "progress", "step": "banner", "status": "completed"}
        )

        await _emit(
            reporter,
            {"type": "progress", "step": "ports", "status": "started"}
        )
        ports_info = await run_masscan(target_ip)
        await _emit(
            reporter,
            {"type": "progress", "step": "ports", "status": "completed"}
        )

        await _emit(
            reporter,
            {"type": "progress", "step": "whois", "status": "started"}
        )
        whois_info = await fetch_asn_whois(target_ip)
        await _emit(
            reporter,
            {"type": "progress", "step": "whois", "status": "completed"}
        )

        await _emit(
            reporter,
            {"type": "progress", "step": "geoip", "status": "started"}
        )
        geo_info = await fetch_geoip(target_ip)
        await _emit(
            reporter,
            {"type": "progress", "step": "geoip", "status": "completed"}
        )

    live_result: dict[str, Any] = {
        "domain": domain,
        "created": now_dt.isoformat(),
        "updated": now_dt.isoformat(),
        "a_record": dns_records.get("a_record"),
        "aaaa_record": dns_records.get("aaaa_record"),
        "ns_record": dns_records.get("ns_record"),
        "mx_record": dns_records.get("mx_record"),
        "soa_record": dns_records.get("soa_record"),
        "cname_record": dns_records.get("cname_record"),
        "txt_record": dns_records.get("txt_record"),
        "header": headers,
        "ports": ports_info,
        "whois": whois_info,
        "geo": geo_info,
        "banner": banner_info,
        "ssl": ssl_info,
        "qrcode": qrcode_info,
    }

    try:
        await _persist_live_result_postgres(
            postgres_session, live_result, now_dt
        )
        await _emit(
            reporter,
            {"type": "progress", "step": "postgres", "status": "stored"},
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to persist live scan for %s", domain)
        await _emit(
            reporter,
            {
                "type": "error",
                "message": "postgres_write_failed",
                "domain": domain,
                "detail": str(exc),
            },
        )

    await _emit(reporter, {"type": "result", "data": live_result})
    return live_result


async def _persist_live_result_postgres(
    session: AsyncSession,
    result: dict[str, Any],
    timestamp: datetime,
) -> None:
    domain_name = result.get("domain")
    if not domain_name:
        return

    updated_at = timestamp.astimezone(timezone.utc).replace(tzinfo=None)

    domain_stmt = select(Domain).where(
        func.lower(Domain.name) == domain_name.lower()
    )
    existing = await session.exec(domain_stmt)
    domain = existing.scalar_one_or_none()

    if domain is None:
        domain = Domain(
            name=domain_name, created_at=updated_at, updated_at=updated_at
        )
        session.add(domain)
        try:
            await session.flush()
        except Exception as e:
            # Handle unique constraint violation from another process
            error_str = str(e).lower()
            if ("uniqueviolationerror" in str(type(e)).lower() or
                    "unique constraint" in error_str):
                await session.rollback()
                # Retry select to get domain inserted by another process
                existing = await session.exec(domain_stmt)
                domain = existing.scalar_one_or_none()
                if domain is None:
                    raise  # If still not found, re-raise the original error
                domain.updated_at = updated_at
                if not domain.created_at:
                    domain.created_at = updated_at
            else:
                raise
    else:
        domain.updated_at = updated_at
        if not domain.created_at:
            domain.created_at = updated_at

    # Update denormalised fields
    header = result.get("header") or {}
    domain.banner = result.get("banner")
    status = header.get("status")
    domain.header_status = str(status) if status is not None else None
    domain.header_server = header.get("server")
    domain.header_x_powered_by = header.get("x-powered-by")

    geo_info = result.get("geo") or {}
    domain.country = geo_info.get("country")
    domain.country_code = geo_info.get("country_code")
    domain.state = geo_info.get("state")
    domain.city = geo_info.get("city")

    # Clear existing relationships
    await session.exec(delete(ARecord).where(ARecord.domain_id == domain.id))
    await session.exec(
        delete(AAAARecord).where(AAAARecord.domain_id == domain.id)
    )
    await session.exec(delete(NSRecord).where(NSRecord.domain_id == domain.id))
    await session.exec(
        delete(SoaRecord).where(SoaRecord.domain_id == domain.id)
    )
    await session.exec(delete(MXRecord).where(MXRecord.domain_id == domain.id))
    await session.exec(
        delete(CNAMERecord).where(CNAMERecord.domain_id == domain.id)
    )
    await session.exec(
        delete(PortService).where(PortService.domain_id == domain.id)
    )
    await session.exec(
        delete(TXTRecord).where(TXTRecord.domain_id == domain.id)
    )
    await session.exec(
        delete(WhoisRecord).where(WhoisRecord.domain_id == domain.id)
    )
    await session.exec(delete(GeoPoint).where(GeoPoint.domain_id == domain.id))

    ssl_ids = select(SSLData.id).where(SSLData.domain_id == domain.id)
    await session.exec(
        delete(SSLSubjectAltName).where(
            SSLSubjectAltName.ssl_id.in_(ssl_ids)
        )
    )
    await session.exec(delete(SSLData).where(SSLData.domain_id == domain.id))

    # Insert new records based on live data
    for ip in _unique(result.get("a_record")):
        session.add(
            ARecord(
                domain_id=domain.id,
                ip_address=ip,
                created_at=updated_at,
                updated_at=updated_at,
            )
        )

    for ip in _unique(result.get("aaaa_record")):
        session.add(
            AAAARecord(
                domain_id=domain.id,
                ip_address=ip,
                created_at=updated_at,
                updated_at=updated_at,
            )
        )

    for value in _unique(result.get("ns_record")):
        session.add(
            NSRecord(domain_id=domain.id, value=value, updated_at=updated_at)
        )

    for value in _unique(result.get("soa_record")):
        session.add(
            SoaRecord(domain_id=domain.id, value=value, updated_at=updated_at)
        )

    for entry in result.get("mx_record") or []:
        if isinstance(entry, dict):
            exchange = entry.get("exchange")
            priority = entry.get("priority")
        else:
            exchange = entry
            priority = None
        if not exchange:
            continue
        session.add(
            MXRecord(
                domain_id=domain.id,
                exchange=str(exchange),
                priority=int(priority) if priority is not None else None,
                updated_at=updated_at,
            )
        )

    for entry in result.get("ports") or []:
        if not isinstance(entry, dict):
            continue
        port = entry.get("port")
        if port is None:
            continue
        try:
            port_int = int(port)
        except (TypeError, ValueError):
            continue
        session.add(
            PortService(
                domain_id=domain.id,
                port=port_int,
                status=entry.get("status"),
                service=entry.get("service"),
                updated_at=updated_at,
            )
        )

    for target in _unique(result.get("cname_record")):
        session.add(
            CNAMERecord(
                domain_id=domain.id,
                target=target,
                updated_at=updated_at,
            )
        )

    for content in _unique(result.get("txt_record")):
        session.add(
            TXTRecord(
                domain_id=domain.id,
                content=content,
                updated_at=updated_at,
            )
        )

    whois_info = result.get("whois") or {}
    if whois_info:
        session.add(
            WhoisRecord(
                domain_id=domain.id,
                asn=whois_info.get("asn"),
                asn_description=whois_info.get("asn_description"),
                asn_country_code=whois_info.get("asn_country_code"),
                asn_registry=whois_info.get("asn_registry"),
                asn_cidr=whois_info.get("asn_cidr"),
                updated_at=updated_at,
            )
        )

    loc = geo_info.get("loc") if isinstance(geo_info, dict) else None
    coordinates = loc.get("coordinates") if isinstance(loc, dict) else None
    if (
        isinstance(coordinates, (list, tuple))
        and len(coordinates) == 2
    ):
        try:
            longitude = float(coordinates[0])
            latitude = float(coordinates[1])
        except (TypeError, ValueError):
            longitude = latitude = None
    else:
        longitude = latitude = None

    session.add(
        GeoPoint(
            domain_id=domain.id,
            latitude=latitude,
            longitude=longitude,
            country_code=geo_info.get("country_code"),
            country=geo_info.get("country"),
            state=geo_info.get("state"),
            city=geo_info.get("city"),
            updated_at=updated_at,
        )
    )

    ssl_info = result.get("ssl") or {}
    if ssl_info:
        issuer = ssl_info.get("issuer", {})
        subject = ssl_info.get("subject", {})
        not_before = _parse_datetime(ssl_info.get("not_before"))
        not_after = _parse_datetime(ssl_info.get("not_after"))
        ssl_record = SSLData(
            domain_id=domain.id,
            issuer_common_name=issuer.get("common_name"),
            issuer_organizational_unit=issuer.get("organizational_unit_name"),
            issuer_organization=issuer.get("organization_name"),
            subject_common_name=subject.get("common_name"),
            subject_organizational_unit=subject.get(
                "organizational_unit_name"
            ),
            subject_organization=subject.get("organization_name"),
            serial_number=ssl_info.get("serial"),
            not_before=not_before,
            not_after=not_after,
            updated_at=updated_at,
        )
        session.add(ssl_record)
        await session.flush()

        for alt in _unique(ssl_info.get("subject_alt_names")):
            session.add(
                SSLSubjectAltName(ssl_id=ssl_record.id, value=alt)
            )

    await session.commit()


def _unique(values: Any) -> List[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        values = [values]
    cleaned = []
    seen = set()
    for value in values:
        if value in (None, ""):
            continue
        value_str = str(value).strip()
        if value_str and value_str not in seen:
            seen.add(value_str)
            cleaned.append(value_str)
    return cleaned


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(
                value.replace("Z", "+00:00")
            ).replace(tzinfo=None)
        except ValueError:
            return None
    return None
