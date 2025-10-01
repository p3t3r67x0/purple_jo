#!/usr/bin/env python3

"""Replay DNS JSONL observations into PostgreSQL tables."""

from __future__ import annotations

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from sqlmodel import Session, select

from shared.models.postgres import (
    AAAARecord,
    ARecord,
    CNAMERecord,
    Domain,
    MXRecord,
    NSRecord,
    SoaRecord,
)
from tools.sqlmodel_helpers import resolve_sync_dsn, session_scope


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_domain(session: Session, domain: str, now: datetime) -> Domain:
    statement = select(Domain).where(Domain.name == domain)
    domain_obj = session.exec(statement).one_or_none()
    if domain_obj:
        domain_obj.updated_at = now
    else:
        domain_obj = Domain(name=domain, created_at=now, updated_at=now)

    session.add(domain_obj)
    session.flush()
    return domain_obj


def insert_a_record(session: Session, domain_id: int, value: str, now: datetime) -> bool:
    statement = select(ARecord).where(
        ARecord.domain_id == domain_id,
        ARecord.ip_address == value,
    )
    existing = session.exec(statement).one_or_none()
    if existing:
        return False

    session.add(
        ARecord(
            domain_id=domain_id,
            ip_address=value,
            created_at=now,
            updated_at=now,
        )
    )
    return True


def insert_aaaa_record(session: Session, domain_id: int, value: str, now: datetime) -> bool:
    statement = select(AAAARecord).where(
        AAAARecord.domain_id == domain_id,
        AAAARecord.ip_address == value,
    )
    existing = session.exec(statement).one_or_none()
    if existing:
        return False

    session.add(
        AAAARecord(
            domain_id=domain_id,
            ip_address=value,
            created_at=now,
            updated_at=now,
        )
    )
    return True


def insert_ns_record(session: Session, domain_id: int, value: str, now: datetime) -> bool:
    statement = select(NSRecord).where(
        NSRecord.domain_id == domain_id,
        NSRecord.value == value,
    )
    existing = session.exec(statement).one_or_none()
    if existing:
        return False

    session.add(NSRecord(domain_id=domain_id, value=value, updated_at=now))
    return True


def insert_mx_record(
    session: Session, domain_id: int, exchange: str, priority: int, now: datetime
) -> bool:
    statement = select(MXRecord).where(
        MXRecord.domain_id == domain_id,
        MXRecord.exchange == exchange,
        MXRecord.priority == priority,
    )
    existing = session.exec(statement).one_or_none()
    if existing:
        return False

    session.add(
        MXRecord(
            domain_id=domain_id,
            exchange=exchange,
            priority=priority,
            updated_at=now,
        )
    )
    return True


def insert_soa_record(session: Session, domain_id: int, value: str, now: datetime) -> bool:
    statement = select(SoaRecord).where(
        SoaRecord.domain_id == domain_id,
        SoaRecord.value == value,
    )
    existing = session.exec(statement).one_or_none()
    if existing:
        return False

    session.add(SoaRecord(domain_id=domain_id, value=value, updated_at=now))
    return True


def insert_cname_record(session: Session, domain_id: int, target: str, now: datetime) -> bool:
    statement = select(CNAMERecord).where(
        CNAMERecord.domain_id == domain_id,
        CNAMERecord.target == target,
    )
    existing = session.exec(statement).one_or_none()
    if existing:
        return False

    session.add(CNAMERecord(domain_id=domain_id, target=target, updated_at=now))
    return True


def process_record(session: Session, payload: Dict[str, Any], line_no: int) -> bool:
    domain = payload.get("query_name", "").lower().strip(".")
    if not domain:
        print(f"[WARNING] Line {line_no}: missing query_name")
        return False

    record_type = payload.get("resp_type")
    if not record_type:
        print(f"[WARNING] Line {line_no}: missing resp_type for domain {domain}")
        return False

    data = payload.get("data", "")
    if data is None:
        print(f"[WARNING] Line {line_no}: missing data for domain {domain}")
        return False

    now = utcnow()
    domain_obj = ensure_domain(session, domain, now)

    inserted = False

    if record_type == "A":
        value = str(data).lower().strip(".")
        if value:
            inserted = insert_a_record(session, domain_obj.id, value, now)
    elif record_type == "AAAA":
        value = str(data).lower().strip(".")
        if value:
            inserted = insert_aaaa_record(session, domain_obj.id, value, now)
    elif record_type == "CNAME":
        target = str(data).lower().strip(".")
        if target:
            inserted = insert_cname_record(session, domain_obj.id, target, now)
    elif record_type == "NS":
        value = str(data).lower().strip(".")
        if value and "root-servers.net" not in value and "gtld-servers.net" not in value:
            inserted = insert_ns_record(session, domain_obj.id, value, now)
    elif record_type == "MX":
        parts = str(data).split()
        if len(parts) >= 2:
            try:
                priority = int(parts[0])
            except ValueError:
                priority = 0
            exchange = parts[1].lower().strip(".")
            inserted = insert_mx_record(session, domain_obj.id, exchange, priority, now)
    elif record_type == "SOA":
        value = str(data).lower().strip(".")
        if value:
            inserted = insert_soa_record(session, domain_obj.id, value, now)
    else:
        print(f"[INFO] Line {line_no}: unsupported record type {record_type}; skipping")

    return inserted


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import DNS records into PostgreSQL")
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="JSONL file containing DNS responses",
    )
    parser.add_argument(
        "--postgres-dsn",
        dest="postgres_dsn",
        default=None,
        help="PostgreSQL DSN (overrides POSTGRES_DSN env/.env)",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    postgres_dsn = resolve_sync_dsn(args.postgres_dsn)

    total_processed = 0
    total_inserted = 0

    with session_scope(dsn=postgres_dsn) as session:
        with args.input.open("r", encoding="utf-8") as handle:
            for line_no, raw in enumerate(handle, start=1):
                line = raw.strip()
                if not line:
                    continue

                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"[ERROR] Line {line_no}: invalid JSON ({exc})")
                    continue

                changed = process_record(session, payload, line_no)
                session.commit()

                total_processed += 1
                if changed:
                    total_inserted += 1

    print(
        f"[INFO] Processed {total_processed} records; "
        f"{total_inserted} inserts/updates applied"
    )


if __name__ == "__main__":
    CLITool(main).run()
