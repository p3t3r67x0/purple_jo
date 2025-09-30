"""High-level query helpers backed by PostgreSQL models."""

from __future__ import annotations

import logging
import math
import socket
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import selectinload
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.utils import cache_key, cached_paginated_fetch, paginate
from app.models.postgres import (
    AAAARecord,
    ARecord,
    CNAMERecord,
    Domain,
    GeoPoint,
    MXRecord,
    NSRecord,
    TXTRecord,
    PortService,
    SSLData,
    SSLSubjectAltName,
    SubnetLookup,
    WhoisRecord,
)
from app.services import asn_lookup, perform_live_scan, build_domain_graph

logger = logging.getLogger(__name__)

DEFAULT_CACHE_TTL = 300


def _domain_relationship_options() -> Sequence:
    return (
        selectinload(Domain.a_records),
        selectinload(Domain.aaaa_records),
        selectinload(Domain.ns_records),
        selectinload(Domain.soa_records),
        selectinload(Domain.mx_records),
        selectinload(Domain.cname_records),
        selectinload(Domain.txt_records),
        selectinload(Domain.ports),
        selectinload(Domain.geo),
        selectinload(Domain.whois),
        selectinload(Domain.ssl).selectinload(SSLData.subject_alt_names),
    )


def _domain_base_query() -> Any:
    return (
        select(Domain)
        .options(*_domain_relationship_options())
        .execution_options(populate_existing=True)
    )


def _pagination_bounds(page: int, page_size: int) -> Tuple[int, int]:
    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    skip = (safe_page - 1) * safe_page_size
    return skip, safe_page_size


async def _scalar(
    session: AsyncSession,
    stmt,
    default: int = 0,
) -> int:
    result = await session.exec(stmt)
    value = result.scalar()
    return default if value is None else int(value)


def _build_header_payload(domain: Domain) -> Dict[str, Any]:
    header: Dict[str, Any] = {}
    if domain.header_status:
        header["status"] = domain.header_status
    if domain.header_server:
        header["server"] = domain.header_server
    if domain.header_x_powered_by:
        header["x-powered-by"] = domain.header_x_powered_by
    return header


def _serialize_geo(geo: Optional[GeoPoint]) -> Optional[Dict[str, Any]]:
    if geo is None:
        return None

    payload: Dict[str, Any] = {
        "country_code": geo.country_code,
        "country": geo.country,
        "state": geo.state,
        "city": geo.city,
    }
    if geo.longitude is not None and geo.latitude is not None:
        payload["loc"] = {
            "type": "Point",
            "coordinates": [geo.longitude, geo.latitude],
        }
    return payload


def _serialize_whois(
    record: Optional[WhoisRecord],
) -> Optional[Dict[str, Any]]:
    if record is None:
        return None

    return {
        "asn": record.asn,
        "asn_description": record.asn_description,
        "asn_country_code": record.asn_country_code,
        "asn_registry": record.asn_registry,
        "asn_cidr": record.asn_cidr,
        "updated": (
            record.updated_at.isoformat() if record.updated_at else None
        ),
    }


def _serialize_ssl(ssl: Optional[SSLData]) -> Optional[Dict[str, Any]]:
    if ssl is None:
        return None

    payload: Dict[str, Any] = {
        "issuer": {
            "common_name": ssl.issuer_common_name,
            "organization_name": ssl.issuer_organization,
            "organizational_unit_name": ssl.issuer_organizational_unit,
        },
        "subject": {
            "common_name": ssl.subject_common_name,
            "organization_name": ssl.subject_organization,
            "organizational_unit_name": ssl.subject_organizational_unit,
        },
        "serial": ssl.serial_number,
        "ocsp": ssl.ocsp,
        "ca_issuers": ssl.ca_issuers,
        "crl_distribution_points": ssl.crl_distribution_points,
        "not_before": ssl.not_before.isoformat() if ssl.not_before else None,
        "not_after": ssl.not_after.isoformat() if ssl.not_after else None,
    }

    if ssl.subject_alt_names:
        payload["subject_alt_names"] = [
            san.value for san in ssl.subject_alt_names
        ]

    return payload


def _serialize_dns_domain(domain: Domain) -> Dict[str, Any]:
    print(domain)
    payload: Dict[str, Any] = {
        "domain": domain.name,
        "updated": (
            domain.updated_at.isoformat() if domain.updated_at else None
        ),
        "country_code": domain.country_code,
        "country": domain.country,
        "state": domain.state,
        "city": domain.city,
    }

    if domain.banner:
        payload["banner"] = domain.banner

    header = _build_header_payload(domain)
    if header:
        payload["header"] = header

    if domain.a_records:
        payload["a_record"] = [
            record.ip_address for record in domain.a_records
        ]
    if domain.aaaa_records:
        payload["aaaa_record"] = [
            record.ip_address for record in domain.aaaa_records]
    if domain.ns_records:
        payload["ns_record"] = [record.value for record in domain.ns_records]
    if domain.soa_records:
        payload["soa_record"] = [record.value for record in domain.soa_records]
    if domain.mx_records:
        payload["mx_record"] = [
            {
                "exchange": record.exchange,
                "priority": record.priority,
            }
            for record in domain.mx_records
        ]
    if domain.cname_records:
        payload["cname_record"] = [
            record.target for record in domain.cname_records
        ]
    if domain.txt_records:
        payload["txt_record"] = [
            record.content for record in domain.txt_records
        ]
    if domain.ports:
        payload["ports"] = [
            {
                "port": port.port,
                "status": port.status,
                "service": port.service,
            }
            for port in domain.ports
        ]

    geo_payload = _serialize_geo(domain.geo)
    if geo_payload:
        payload["geo"] = geo_payload

    whois_payload = _serialize_whois(domain.whois)
    if whois_payload:
        payload["whois"] = whois_payload

    ssl_payload = _serialize_ssl(domain.ssl)
    if ssl_payload:
        payload["ssl"] = ssl_payload

    return payload


def _domain_search_filter(term: str):
    pattern = f"%{term}%"
    return or_(
        Domain.name.ilike(pattern),
        Domain.cname_records.any(CNAMERecord.target.ilike(pattern)),
        Domain.mx_records.any(MXRecord.exchange.ilike(pattern)),
        Domain.ns_records.any(NSRecord.value.ilike(pattern)),
        Domain.ports.any(PortService.service.ilike(pattern)),
        Domain.ssl.has(
            or_(
                SSLData.subject_common_name.ilike(pattern),
                SSLData.issuer_common_name.ilike(pattern),
                SSLData.subject_alt_names.any(
                    SSLSubjectAltName.value.ilike(pattern)
                ),
            )
        ),
        Domain.banner.ilike(pattern),
    )


async def fetch_query_domain(
    domain: str,
    *,
    session: AsyncSession,
    page: int = 1,
    page_size: int = 25,
):
    term = domain.strip()
    if not term:
        return paginate(page=page, page_size=page_size, total=0, results=[])

    skip, limit = _pagination_bounds(page, page_size)
    filter_expr = _domain_search_filter(term)

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        stmt = (
            _domain_base_query()
            .where(filter_expr)
            .order_by(Domain.updated_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await session.exec(stmt)
        domains = result.scalars().unique().all()
        items = [
            {
                **_serialize_dns_domain(domain_obj),
                "score": 1.0,
            }
            for domain_obj in domains
        ]

        total_stmt = select(func.count(
            func.distinct(Domain.id))).where(filter_expr)
        total = await _scalar(session, total_stmt, default=0)
        return items, total

    key = f"query:{cache_key(term)}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_all_prefix(
    prefix: str,
    *,
    session: AsyncSession,
    page: int = 1,
    page_size: int = 25,
):
    skip, limit = _pagination_bounds(page, page_size)

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        stmt = (
            select(SubnetLookup)
            .options(selectinload(SubnetLookup.domain))
            .where(SubnetLookup.cidr == prefix)
            .order_by(SubnetLookup.updated_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await session.exec(stmt)
        lookups = result.scalars().all()
        items = [
            {
                "cidr": entry.cidr,
                "asn": entry.asn,
                "asn_description": entry.asn_description,
                "ip_start": entry.ip_start,
                "ip_end": entry.ip_end,
                "source": entry.source,
                "updated": entry.updated_at.isoformat()
                if entry.updated_at
                else None,
                "domain": entry.domain.name if entry.domain else None,
            }
            for entry in lookups
        ]

        total_stmt = (
            select(func.count())
            .select_from(SubnetLookup)
            .where(SubnetLookup.cidr == prefix)
        )
        total = await _scalar(session, total_stmt, default=0)
        return items, total

    key = f"subnet:{cache_key(prefix)}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_latest_dns(
    *,
    session: AsyncSession,
    page: int = 1,
    page_size: int = 10,
):
    skip, limit = _pagination_bounds(page, page_size)

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        items, total = await _fetch_latest_dns_postgres(session, skip, limit)
        return items, total

    key = f"dns:latest:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_latest_cidr(
    *,
    session: AsyncSession,
    page: int = 1,
    page_size: int = 50,
):
    skip, limit = _pagination_bounds(page, page_size)

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        items, total = await _fetch_latest_cidr_postgres(session, skip, limit)
        return items, total

    key = f"cidr:latest:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_latest_ipv4(
    *,
    session: AsyncSession,
    page: int = 1,
    page_size: int = 60,
):
    skip, limit = _pagination_bounds(page, page_size)

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        items, total = await _fetch_latest_ipv4_postgres(session, skip, limit)
        return items, total

    key = f"ipv4:latest:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


def _normalise_asn(value: str) -> str:
    clean = value.strip().upper()
    if clean.startswith("AS"):
        clean = clean[2:]
    return clean


async def fetch_latest_asn(
    *,
    session: AsyncSession,
    page: int = 1,
    page_size: int = 50,
    country_code: Optional[str] = None,
):
    skip, limit = _pagination_bounds(page, page_size)
    query_country = country_code.upper() if country_code else None

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        items, total = await _fetch_latest_asn_postgres(
            session,
            skip,
            limit,
            query_country,
        )
        return items, total

    country_key = query_country if query_country else "all"
    key = f"asn:latest:{country_key}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_one_ip(
    session: AsyncSession,
    ip: str,
    *,
    page: int = 1,
    page_size: int = 1,
):
    skip, limit = _pagination_bounds(page, page_size)
    filter_expr = Domain.a_records.any(ARecord.ip_address == ip)

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        stmt = (
            _domain_base_query()
            .where(filter_expr)
            .order_by(Domain.updated_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await session.exec(stmt)
        domains = result.scalars().unique().all()
        if domains:
            items = [_serialize_dns_domain(domain_obj)
                     for domain_obj in domains]
            total_stmt = select(func.count(func.distinct(Domain.id))).where(
                filter_expr
            )
            total = await _scalar(session, total_stmt, default=0)
            return items, total

        if page > 1:
            return [], 0

        logger.info("Falling back to ASN lookup for %s", ip)
        lookup = asn_lookup(ip)
        try:
            host = socket.gethostbyaddr(ip)[0]
        except Exception:  # noqa: BLE001
            host = None

        now = datetime.utcnow().isoformat()
        fallback = {
            "ip": ip,
            "host": host,
            "updated": now,
            "asn": lookup.get("asn"),
            "name": lookup.get("name"),
            "cidr": [lookup.get("prefix")],
        }
        return [fallback], 1

    key = f"ip:{ip}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


def _parse_geo_coordinates(raw: str) -> Optional[Tuple[float, float]]:
    try:
        first, second = map(float, raw.split(","))
    except ValueError:
        return None

    def _within_lat(val: float) -> bool:
        return -90.0 <= val <= 90.0

    def _within_lon(val: float) -> bool:
        return -180.0 <= val <= 180.0

    lat: Optional[float]
    lon: Optional[float]

    if _within_lat(first) and _within_lon(second):
        lat, lon = first, second
    elif _within_lat(second) and _within_lon(first):
        lat, lon = second, first
    else:
        return None

    return lat, lon


def _geo_radius_filter(lat: float, lon: float, radius_km: float = 50.0):
    lat_delta = radius_km / 110.574
    lon_scale = math.cos(math.radians(lat)) * 111.320
    lon_delta = radius_km / lon_scale if lon_scale else 180.0

    return Domain.geo.has(
        and_(
            GeoPoint.latitude.is_not(None),
            GeoPoint.longitude.is_not(None),
            GeoPoint.latitude >= lat - lat_delta,
            GeoPoint.latitude <= lat + lat_delta,
            GeoPoint.longitude >= lon - lon_delta,
            GeoPoint.longitude <= lon + lon_delta,
        )
    )


def _build_match_filter(condition: str, query: str):
    condition = condition.lower()

    if condition in {"site", "domain"}:
        return func.lower(Domain.name) == query.lower()
    if condition == "registry":
        return Domain.whois.has(WhoisRecord.asn_registry.ilike(query))
    if condition == "port":
        try:
            port_value = int(query)
        except ValueError:
            return None
        return Domain.ports.any(PortService.port == port_value)
    if condition == "status":
        return Domain.header_status == query
    if condition == "service":
        return Domain.header_x_powered_by.ilike(f"%{query}%")
    if condition == "banner":
        return Domain.banner.ilike(f"%{query}%")
    if condition == "ssl":
        pattern = f"%{query}%"
        return Domain.ssl.has(
            or_(
                SSLData.subject_common_name.ilike(pattern),
                SSLData.subject_alt_names.any(
                    SSLSubjectAltName.value.ilike(pattern)
                ),
            )
        )
    if condition == "before":
        try:
            dt = datetime.strptime(query, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        return Domain.ssl.has(SSLData.not_before >= dt)
    if condition == "after":
        try:
            dt = datetime.strptime(query, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        return Domain.ssl.has(SSLData.not_after <= dt)
    if condition == "ca":
        return Domain.ssl.has(SSLData.ca_issuers.ilike(f"%{query}%"))
    if condition == "issuer":
        pattern = f"%{query}%"
        return Domain.ssl.has(
            or_(
                SSLData.issuer_organization.ilike(pattern),
                SSLData.issuer_common_name.ilike(pattern),
            )
        )
    if condition == "unit":
        pattern = f"%{query}%"
        return Domain.ssl.has(
            or_(
                SSLData.issuer_organizational_unit.ilike(pattern),
                SSLData.subject_organizational_unit.ilike(pattern),
            )
        )
    if condition == "ocsp":
        return Domain.ssl.has(SSLData.ocsp.ilike(f"%{query}%"))
    if condition == "crl":
        return Domain.ssl.has(
            SSLData.crl_distribution_points.ilike(f"%{query}%")
        )
    if condition == "country":
        upper = query.upper()
        return or_(
            Domain.country_code == upper,
            Domain.whois.has(WhoisRecord.asn_country_code == upper),
        )
    if condition == "state":
        return Domain.geo.has(GeoPoint.state.ilike(f"%{query}%"))
    if condition == "city":
        return Domain.geo.has(GeoPoint.city.ilike(f"%{query}%"))
    if condition == "asn":
        normalised = _normalise_asn(query)
        if not normalised:
            return None
        return Domain.whois.has(WhoisRecord.asn == normalised)
    if condition == "org":
        pattern = f"%{query}%"
        return or_(
            Domain.whois.has(WhoisRecord.asn_description.ilike(pattern)),
            Domain.ssl.has(SSLData.subject_organization.ilike(pattern)),
        )
    if condition == "cidr":
        return Domain.whois.has(WhoisRecord.asn_cidr == query)
    if condition == "cname":
        return Domain.cname_records.any(CNAMERecord.target == query)
    if condition == "mx":
        return Domain.mx_records.any(MXRecord.exchange == query)
    if condition == "ns":
        return Domain.ns_records.any(NSRecord.value == query)
    if condition == "server":
        return Domain.header_server.ilike(f"%{query}%")
    if condition == "ipv4":
        return Domain.a_records.any(ARecord.ip_address == query)
    if condition == "ipv6":
        return Domain.aaaa_records.any(AAAARecord.ip_address == query)
    if condition == "loc":
        parsed = _parse_geo_coordinates(query)
        if not parsed:
            return None
        return _geo_radius_filter(*parsed)
    return None


async def fetch_match_condition(
    condition: str,
    query: str,
    *,
    session: AsyncSession,
    page: int = 1,
    page_size: int = 30,
):
    if not query:
        return paginate(page=page, page_size=page_size, total=0, results=[])

    filter_expr = _build_match_filter(condition, query)
    if filter_expr is None:
        return paginate(page=page, page_size=page_size, total=0, results=[])

    skip, limit = _pagination_bounds(page, page_size)
    condition_key = condition.lower()

    async def loader() -> Tuple[List[Dict[str, Any]], int]:
        stmt = (
            _domain_base_query()
            .where(filter_expr)
            .order_by(Domain.updated_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await session.exec(stmt)
        domains = result.scalars().unique().all()

        if not domains and condition_key in {"site", "domain"} and page == 1:
            logger.info("Running live scan fallback for %s", query)
            scan_result = await perform_live_scan(
                query,
                postgres_session=session,
                reporter=None,
            )
            refreshed = await session.exec(stmt)
            domains = refreshed.scalars().unique().all()
            if not domains:
                return [scan_result], 1

        items = [_serialize_dns_domain(domain_obj) for domain_obj in domains]
        total_stmt = select(func.count(
            func.distinct(Domain.id))).where(filter_expr)
        total = await _scalar(session, total_stmt, default=0)
        return items, total

    key = f"match:{condition_key}:{cache_key(query)}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def extract_graph(
    domain: str,
    *,
    session: AsyncSession,
) -> Dict[str, Any]:
    return await build_domain_graph(session, domain)


async def _fetch_latest_ipv4_postgres(
    session: AsyncSession,
    skip: int,
    limit: int,
) -> Tuple[List[Dict[str, Any]], int]:
    stmt = (
        select(ARecord.ip_address, Domain.country_code)
        .join(Domain, ARecord.domain_id == Domain.id)
        .order_by(ARecord.updated_at.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await session.exec(stmt)
    rows = result.all()
    items = [
        {
            "a_record": ip_address,
            "country_code": country_code,
        }
        for ip_address, country_code in rows
    ]

    total_stmt = select(func.count()).select_from(ARecord)
    total = await _scalar(session, total_stmt, default=0)
    return items, total


async def _fetch_latest_dns_postgres(
    session: AsyncSession,
    skip: int,
    limit: int,
) -> Tuple[List[Dict[str, Any]], int]:
    stmt = (
        _domain_base_query()
        .where(Domain.a_records.any())
        .order_by(Domain.updated_at.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await session.exec(stmt)
    domains = result.scalars().unique().all()
    items = [_serialize_dns_domain(domain) for domain in domains]

    total_stmt = select(func.count()).select_from(
        Domain).where(Domain.a_records.any())
    total = await _scalar(session, total_stmt, default=0)
    return items, total


async def _fetch_latest_cidr_postgres(
    session: AsyncSession,
    skip: int,
    limit: int,
) -> Tuple[List[Dict[str, Any]], int]:
    stmt = (
        select(WhoisRecord)
        .options(selectinload(WhoisRecord.domain))
        .where(WhoisRecord.asn_cidr.is_not(None))
        .order_by(WhoisRecord.updated_at.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await session.exec(stmt)
    rows = result.scalars().all()
    items = [
        {
            "domain": whois.domain.name,
            "whois": {
                "asn_cidr": whois.asn_cidr,
                "asn_country_code": whois.asn_country_code,
            },
            "updated": whois.updated_at.isoformat() if whois.updated_at else None,
        }
        for whois in rows
    ]

    total_stmt = select(func.count()).select_from(WhoisRecord).where(
        WhoisRecord.asn_cidr.is_not(None)
    )
    total = await _scalar(session, total_stmt, default=0)
    return items, total


async def _fetch_latest_asn_postgres(
    session: AsyncSession,
    skip: int,
    limit: int,
    country_code: Optional[str],
) -> Tuple[List[Dict[str, Any]], int]:
    filters = [WhoisRecord.asn.is_not(None)]
    if country_code:
        filters.append(WhoisRecord.asn_country_code == country_code)

    stmt = (
        select(WhoisRecord)
        .options(selectinload(WhoisRecord.domain))
        .where(*filters)
        .order_by(WhoisRecord.updated_at.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await session.exec(stmt)
    rows = result.scalars().all()
    items = [
        {
            "domain": whois.domain.name,
            "whois": {
                "asn": whois.asn,
                "asn_country_code": whois.asn_country_code,
                "asn_description": whois.asn_description,
            },
            "updated": whois.updated_at.isoformat() if whois.updated_at else None,
        }
        for whois in rows
    ]

    total_stmt = select(func.count()).select_from(WhoisRecord).where(*filters)
    total = await _scalar(session, total_stmt, default=0)
    return items, total
