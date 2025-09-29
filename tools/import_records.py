#!/usr/bin/env python3

"""Replay DNS JSONL observations into PostgreSQL tables."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import psycopg

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_file(path: Path) -> dict[str, str]:
    env_vars: dict[str, str] = {}
    if not path.exists():
        return env_vars

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_vars[key.strip()] = value.strip().strip('"\'')
    return env_vars


def resolve_dsn(explicit: str | None = None) -> str:
    if explicit:
        dsn = explicit
    elif "POSTGRES_DSN" in os.environ:
        dsn = os.environ["POSTGRES_DSN"]
    else:
        env_vars = _parse_env_file(ENV_PATH)
        dsn = env_vars.get("POSTGRES_DSN")

    if not dsn:
        raise ValueError("POSTGRES_DSN not provided via flag, env var, or .env file")

    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    elif dsn.startswith("postgresql+psycopg://"):
        dsn = "postgresql://" + dsn[len("postgresql+psycopg://") :]
    elif "://" not in dsn:
        dsn = f"postgresql://{dsn}"

    return dsn


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_domain(cur: psycopg.Cursor, domain: str, now: datetime) -> int:
    cur.execute(
        """
        INSERT INTO domains (name, created_at, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE
        SET updated_at = EXCLUDED.updated_at
        RETURNING id
        """,
        (domain, now, now),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Failed to ensure domain {domain}")
    return int(row[0])


def insert_a_record(cur: psycopg.Cursor, domain_id: int, value: str, now: datetime) -> bool:
    cur.execute(
        """
        INSERT INTO a_records (domain_id, ip_address, created_at, updated_at)
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM a_records WHERE domain_id = %s AND ip_address = %s
        )
        """,
        (domain_id, value, now, now, domain_id, value),
    )
    return cur.rowcount == 1


def insert_aaaa_record(cur: psycopg.Cursor, domain_id: int, value: str, now: datetime) -> bool:
    cur.execute(
        """
        INSERT INTO aaaa_records (domain_id, ip_address, created_at, updated_at)
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM aaaa_records WHERE domain_id = %s AND ip_address = %s
        )
        """,
        (domain_id, value, now, now, domain_id, value),
    )
    return cur.rowcount == 1


def insert_ns_record(cur: psycopg.Cursor, domain_id: int, value: str, now: datetime) -> bool:
    cur.execute(
        """
        INSERT INTO ns_records (domain_id, value, updated_at)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM ns_records WHERE domain_id = %s AND value = %s
        )
        """,
        (domain_id, value, now, domain_id, value),
    )
    return cur.rowcount == 1


def insert_mx_record(
    cur: psycopg.Cursor, domain_id: int, exchange: str, priority: int, now: datetime
) -> bool:
    cur.execute(
        """
        INSERT INTO mx_records (domain_id, exchange, priority, updated_at)
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM mx_records
            WHERE domain_id = %s AND exchange = %s AND COALESCE(priority, -1) = COALESCE(%s, -1)
        )
        """,
        (domain_id, exchange, priority, now, domain_id, exchange, priority),
    )
    return cur.rowcount == 1


def insert_soa_record(cur: psycopg.Cursor, domain_id: int, value: str, now: datetime) -> bool:
    cur.execute(
        """
        INSERT INTO soa_records (domain_id, value, updated_at)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM soa_records WHERE domain_id = %s AND value = %s
        )
        """,
        (domain_id, value, now, domain_id, value),
    )
    return cur.rowcount == 1


def insert_cname_record(cur: psycopg.Cursor, domain_id: int, target: str, now: datetime) -> bool:
    cur.execute(
        """
        INSERT INTO cname_records (domain_id, target, updated_at)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM cname_records WHERE domain_id = %s AND target = %s
        )
        """,
        (domain_id, target, now, domain_id, target),
    )
    return cur.rowcount == 1


def update_domain_timestamp(cur: psycopg.Cursor, domain_id: int, now: datetime) -> None:
    cur.execute(
        """
        UPDATE domains
        SET updated_at = %s
        WHERE id = %s
        """,
        (now, domain_id),
    )


def process_record(cur: psycopg.Cursor, payload: Dict[str, Any], line_no: int) -> bool:
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
    domain_id = ensure_domain(cur, domain, now)

    inserted = False

    if record_type == "A":
        value = str(data).lower().strip(".")
        if value:
            inserted = insert_a_record(cur, domain_id, value, now)
    elif record_type == "AAAA":
        value = str(data).lower().strip(".")
        if value:
            inserted = insert_aaaa_record(cur, domain_id, value, now)
    elif record_type == "CNAME":
        target = str(data).lower().strip(".")
        if target:
            inserted = insert_cname_record(cur, domain_id, target, now)
    elif record_type == "NS":
        value = str(data).lower().strip(".")
        if value and "root-servers.net" not in value and "gtld-servers.net" not in value:
            inserted = insert_ns_record(cur, domain_id, value, now)
    elif record_type == "MX":
        parts = str(data).split()
        if len(parts) >= 2:
            try:
                priority = int(parts[0])
            except ValueError:
                priority = 0
            exchange = parts[1].lower().strip(".")
            inserted = insert_mx_record(cur, domain_id, exchange, priority, now)
    elif record_type == "SOA":
        value = str(data).lower().strip(".")
        if value:
            inserted = insert_soa_record(cur, domain_id, value, now)
    else:
        print(f"[INFO] Line {line_no}: unsupported record type {record_type}; skipping")

    if inserted:
        update_domain_timestamp(cur, domain_id, now)
        print(f"INFO: updated {record_type} record for domain {domain}")

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
    postgres_dsn = resolve_dsn(args.postgres_dsn)

    total_processed = 0
    total_inserted = 0

    with psycopg.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
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

                    changed = process_record(cur, payload, line_no)
                    conn.commit()

                    total_processed += 1
                    if changed:
                        total_inserted += 1

    print(
        f"[INFO] Processed {total_processed} records; "
        f"{total_inserted} inserts/updates applied"
    )


if __name__ == "__main__":
    main()
