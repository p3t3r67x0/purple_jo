#!/usr/bin/env python3

"""Insert IPv4 addresses or CIDR ranges into subnet_lookups."""

from __future__ import annotations

import argparse
import ipaddress
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple

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


def load_entries(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            entry = line.strip()
            if not entry or entry.startswith("#"):
                continue
            yield entry


def parse_entry(entry: str) -> Tuple[str, str, str]:
    """Return (cidr, ip_start, ip_end) tuple for an IP or CIDR entry."""

    if "/" in entry:
        network = ipaddress.ip_network(entry, strict=False)
        cidr = str(network)
        ip_start = str(network.network_address)
        ip_end = str(network.broadcast_address)
    else:
        ip = ipaddress.ip_address(entry)
        cidr = f"{ip}/32"
        ip_start = ip_end = str(ip)

    return cidr, ip_start, ip_end


def insert_subnet(conn: psycopg.Connection, cidr: str, ip_start: str, ip_end: str) -> bool:
    now = utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO subnet_lookups (cidr, ip_start, ip_end, source, created_at, updated_at)
            SELECT %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM subnet_lookups WHERE cidr = %s
            )
            """,
            (
                cidr,
                ip_start,
                ip_end,
                "import_ip",
                now,
                now,
                cidr,
            ),
        )
        inserted = cur.rowcount == 1
    conn.commit()
    return inserted


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import IPv4/CIDR data into PostgreSQL")
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Plaintext file containing IPv4 addresses or CIDR ranges",
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

    entries = list(load_entries(args.input))
    if not entries:
        print("[INFO] No IP addresses or CIDR ranges to import")
        return

    print(f"[INFO] Importing {len(entries)} subnet entries into PostgreSQL")

    with psycopg.connect(postgres_dsn) as conn:
        for entry in entries:
            cidr, ip_start, ip_end = parse_entry(entry)
            inserted = insert_subnet(conn, cidr, ip_start, ip_end)
            if inserted:
                print(f"INFO: Added subnet {cidr} ({ip_start} - {ip_end})")
            else:
                print(f"INFO: Subnet {cidr} already exists; skipping")

    print("[INFO] Subnet import completed")


if __name__ == "__main__":
    main()
