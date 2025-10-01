"""One-shot migration script to move Mongo DNS documents into PostgreSQL."""

from __future__ import annotations

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

from importlib import import_module

try:
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

bootstrap.setup()

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import delete, select
from sqlmodel.ext.asyncio.session import AsyncSession

from tools.async_sqlmodel_helpers import get_session_factory, init_db
from shared.models.postgres import (
    AAAARecord,
    ARecord,
    CNAMERecord,
    Domain,
    GeoPoint,
    MXRecord,
    NSRecord,
    PortService,
    SoaRecord,
    SSLData,
    SSLSubjectAltName,
    WhoisRecord,
)

logger = logging.getLogger("mongo_to_postgres")


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        trimmed = value.strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(trimmed, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(trimmed.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _as_iterable(value: Any) -> Iterable[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_text(value: Any, max_length: int | None = None) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list):
        for item in value:
            if item:
                value = item
                break
        else:
            return None
    text = str(value).strip()
    if not text:
        return None
    if max_length and len(text) > max_length:
        return text[:max_length]
    return text


def _unique(values: Iterable[Any]) -> list[Any]:
    seen: set[Any] = set()
    unique_list: list[Any] = []
    for value in values:
        if value in seen or value in (None, ""):
            continue
        seen.add(value)
        unique_list.append(value)
    return unique_list


async def migrate_dns_document(session: AsyncSession, doc: dict[str, Any]) -> None:
    name = doc.get("domain")
    if not name:
        return

    updated_at = _parse_datetime(doc.get("updated")) or _utc_now()
    created_at = _parse_datetime(doc.get("created")) or updated_at
    updated_at_naive = _to_naive_utc(updated_at)
    created_at_naive = _to_naive_utc(created_at)

    existing = await session.exec(select(Domain).where(Domain.name == name))
    domain = existing.scalar_one_or_none()

    if domain is None:
        domain = Domain(name=name, created_at=created_at_naive, updated_at=updated_at_naive)
        session.add(domain)
        await session.flush()
    else:
        domain.updated_at = updated_at_naive
        if not domain.created_at:
            domain.created_at = created_at_naive

    # Top-level annotations
    domain.banner = _normalize_text(doc.get("banner"))
    domain.country_code = _normalize_text(doc.get("country_code"), max_length=8)
    domain.country = _normalize_text(doc.get("country"), max_length=255)
    domain.state = _normalize_text(doc.get("state"), max_length=255)
    domain.city = _normalize_text(doc.get("city"), max_length=255)

    header = doc.get("header") or {}
    status = header.get("status") if header.get("status") is not None else header.get("code")
    domain.header_status = _normalize_text(status, max_length=16)
    domain.header_server = _normalize_text(header.get("server"), max_length=255)
    domain.header_x_powered_by = _normalize_text(
        header.get("x-powered-by") or header.get("x_powered_by"),
        max_length=255,
    )

    # Clear existing relational rows
    await session.exec(delete(ARecord).where(ARecord.domain_id == domain.id))
    await session.exec(delete(AAAARecord).where(AAAARecord.domain_id == domain.id))
    await session.exec(delete(NSRecord).where(NSRecord.domain_id == domain.id))
    await session.exec(delete(SoaRecord).where(SoaRecord.domain_id == domain.id))
    await session.exec(delete(MXRecord).where(MXRecord.domain_id == domain.id))
    await session.exec(delete(CNAMERecord).where(CNAMERecord.domain_id == domain.id))
    await session.exec(delete(PortService).where(PortService.domain_id == domain.id))
    await session.exec(delete(WhoisRecord).where(WhoisRecord.domain_id == domain.id))
    await session.exec(delete(GeoPoint).where(GeoPoint.domain_id == domain.id))
    ssl_ids = select(SSLData.id).where(SSLData.domain_id == domain.id).scalar_subquery()
    await session.exec(delete(SSLSubjectAltName).where(SSLSubjectAltName.ssl_id.in_(ssl_ids)))
    await session.exec(delete(SSLData).where(SSLData.domain_id == domain.id))

    # A records
    a_values = _unique(_as_iterable(doc.get("a_record")))
    a_records = [
        ARecord(
            domain_id=domain.id,
            ip_address=str(ip).strip(),
            created_at=updated_at_naive,
            updated_at=updated_at_naive,
        )
        for ip in a_values
    ]
    if a_records:
        session.add_all(a_records)

    # AAAA records
    aaaa_values = _unique(_as_iterable(doc.get("aaaa_record")))
    aaaa_records = [
        AAAARecord(
            domain_id=domain.id,
            ip_address=str(ip).strip(),
            created_at=updated_at_naive,
            updated_at=updated_at_naive,
        )
        for ip in aaaa_values
    ]
    if aaaa_records:
        session.add_all(aaaa_records)

    # NS records
    ns_values = _unique(_as_iterable(doc.get("ns_record")))
    ns_records = [
        NSRecord(domain_id=domain.id, value=str(value).strip(), updated_at=updated_at_naive)
        for value in ns_values
    ]
    if ns_records:
        session.add_all(ns_records)

    # SOA records
    soa_values = _unique(_as_iterable(doc.get("soa_record")))
    soa_records = [
        SoaRecord(domain_id=domain.id, value=str(value).strip(), updated_at=updated_at_naive)
        for value in soa_values
    ]
    if soa_records:
        session.add_all(soa_records)

    # MX records
    mx_values = _as_iterable(doc.get("mx_record"))
    mx_records: list[MXRecord] = []
    for entry in mx_values:
        if isinstance(entry, dict):
            exchange = entry.get("exchange") or entry.get("target")
            priority = entry.get("priority")
        else:
            exchange = str(entry).strip()
            priority = None
        if not exchange:
            continue
        mx_records.append(
        MXRecord(
            domain_id=domain.id,
            exchange=str(exchange).strip(),
            priority=int(priority) if priority is not None else None,
            updated_at=updated_at_naive,
            )
        )
    if mx_records:
        session.add_all(mx_records)

    # CNAME records
    cname_values = _as_iterable(doc.get("cname_record"))
    cname_records: list[CNAMERecord] = []
    for entry in cname_values:
        if isinstance(entry, dict):
            target = entry.get("target") or entry.get("value")
        else:
            target = str(entry).strip()
        if not target:
            continue
        cname_records.append(
        CNAMERecord(
            domain_id=domain.id,
            target=str(target).strip(),
            updated_at=updated_at_naive,
            )
        )
    if cname_records:
        session.add_all(cname_records)

    # Port services
    port_entries = _as_iterable(doc.get("ports"))
    port_records: list[PortService] = []
    for entry in port_entries:
        if not isinstance(entry, dict):
            continue
        port = entry.get("port")
        if port is None:
            continue
        try:
            port_int = int(port)
        except (TypeError, ValueError):
            continue
        port_records.append(
        PortService(
            domain_id=domain.id,
            port=port_int,
            protocol=str(entry.get("proto", "tcp")),
            status=entry.get("status"),
            updated_at=updated_at_naive,
            )
        )
    if port_records:
        session.add_all(port_records)

    # Whois
    whois_data = doc.get("whois") or {}
    if whois_data:
        whois_record = WhoisRecord(
            domain_id=domain.id,
            asn=_normalize_text(whois_data.get("asn"), max_length=32),
            asn_description=_normalize_text(whois_data.get("asn_description")),
            asn_country_code=_normalize_text(whois_data.get("asn_country_code"), max_length=8),
            asn_registry=_normalize_text(whois_data.get("asn_registry"), max_length=32),
            asn_cidr=_normalize_text(whois_data.get("asn_cidr"), max_length=64),
            updated_at=updated_at_naive,
        )
        session.add(whois_record)

    # Geo
    geo_data = doc.get("geo") or {}
    if geo_data:
        loc = geo_data.get("loc") or {}
        coordinates = loc.get("coordinates") if isinstance(loc, dict) else None
        longitude: Optional[float]
        latitude: Optional[float]
        if (
            isinstance(coordinates, (list, tuple))
            and len(coordinates) == 2
            and all(isinstance(val, (int, float)) for val in coordinates)
        ):
            longitude = float(coordinates[0])
            latitude = float(coordinates[1])
        else:
            longitude = latitude = None
        geo_record = GeoPoint(
            domain_id=domain.id,
            latitude=latitude,
            longitude=longitude,
            country_code=geo_data.get("country_code"),
            country=geo_data.get("country"),
            state=geo_data.get("state"),
            city=geo_data.get("city"),
            updated_at=updated_at_naive,
        )
        session.add(geo_record)

    # SSL
    ssl_data = doc.get("ssl") or {}
    if ssl_data:
        not_before_dt = _parse_datetime(ssl_data.get("not_before"))
        not_after_dt = _parse_datetime(ssl_data.get("not_after"))
        ssl_record = SSLData(
            domain_id=domain.id,
            issuer_common_name=ssl_data.get("issuer", {}).get("common_name"),
            issuer_organization=ssl_data.get("issuer", {}).get("organization_name"),
            issuer_organizational_unit=ssl_data.get("issuer", {}).get("organizational_unit_name"),
            subject_common_name=ssl_data.get("subject", {}).get("common_name"),
            subject_organization=ssl_data.get("subject", {}).get("organization_name"),
            subject_organizational_unit=ssl_data.get("subject", {}).get("organizational_unit_name"),
            serial_number=ssl_data.get("serial"),
            ocsp=_first_or_str(ssl_data.get("ocsp")),
            ca_issuers=_first_or_str(ssl_data.get("ca_issuers")),
            crl_distribution_points=_join_list(ssl_data.get("crl_distribution_points")),
            not_before=_to_naive_utc(not_before_dt) if not_before_dt else None,
            not_after=_to_naive_utc(not_after_dt) if not_after_dt else None,
            updated_at=updated_at_naive,
        )
        session.add(ssl_record)
        await session.flush()

        alt_names = [
            SSLSubjectAltName(ssl_id=ssl_record.id, value=str(name).strip())
            for name in _unique(_as_iterable(ssl_data.get("subject_alt_names")))
            if name
        ]
        if alt_names:
            session.add_all(alt_names)


def _first_or_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list) and value:
        return str(value[0])
    return str(value)


def _join_list(value: Any) -> Optional[str]:
    items = [str(item).strip() for item in _as_iterable(value) if item]
    return "\n".join(items) if items else None


async def migrate(
    mongo_uri: str, 
    mongo_db: str, 
    limit: Optional[int], 
    batch_size: int, 
    dry_run: bool, 
    resume_from: Optional[str]
) -> None:
    client = AsyncIOMotorClient(mongo_uri)
    mongo = client[mongo_db]

    await init_db()
    session_factory = get_session_factory()

    query: dict[str, Any] = {}
    if resume_from:
        query["domain"] = {"$gte": resume_from}

    total = await mongo.dns.count_documents(query)
    logger.info("Starting migration | total=%s batch_size=%s dry_run=%s", total, batch_size, dry_run)

    processed = 0
    migrated = 0
    failed = 0

    async with await mongo.client.start_session() as session_ctx:
        cursor = (
            mongo.dns.find(query, session=session_ctx, no_cursor_timeout=True)
            .sort("domain", 1)
            .batch_size(batch_size)
        )

        try:
            async for doc in cursor:
                processed += 1
                if limit and migrated >= limit:
                    break

                async with session_factory() as session:
                    try:
                        await migrate_dns_document(session, doc)
                        if dry_run:
                            await session.rollback()
                        else:
                            await session.commit()
                        migrated += 1
                    except Exception as exc:  # noqa: BLE001
                        failed += 1
                        await session.rollback()
                        logger.exception("Failed to migrate domain=%s", doc.get("domain"))

                if processed % batch_size == 0:
                    logger.info(
                        "Progress | processed=%s migrated=%s failed=%s",
                        processed,
                        migrated,
                        failed,
                    )

        finally:
            await cursor.close()
            client.close()

    skipped = processed - migrated - failed
    logger.info(
        "Migration complete | processed=%s migrated=%s failed=%s skipped=%s",
        processed,
        migrated,
        failed,
        skipped,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate Mongo DNS documents to PostgreSQL")
    parser.add_argument(
        "--mongo-uri", 
        type=str, 
        default="mongodb://localhost:27017", 
        help="MongoDB connection URI"
    )
    parser.add_argument(
        "--mongo-db", 
        type=str, 
        default="ip_data", 
        help="MongoDB database name"
    )
    parser.add_argument("--limit", type=int, default=None, help="Stop after migrating N domains")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of Mongo documents to fetch per batch",
    )
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to PostgreSQL")
    parser.add_argument(
        "--resume-from",
        type=str,
        default=None,
        help="Resume from a specific domain (inclusive)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="[%(levelname)s] %(name)s: %(message)s",
    )


async def async_main() -> None:
    args = parse_args()
    configure_logging(args.log_level.upper())
    await migrate(
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        limit=args.limit,
        batch_size=max(1, args.batch_size),
        dry_run=args.dry_run,
        resume_from=args.resume_from,
    )


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:  # pragma: no cover - CLI convenience
        logger.warning("Migration interrupted by user")


if __name__ == "__main__":
    CLITool(main).run()
