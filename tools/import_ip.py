#!/usr/bin/env python3

"""Insert IPv4 addresses or CIDR ranges into subnet_lookups."""

from __future__ import annotations

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

import argparse
import ipaddress
from pathlib import Path
from typing import Iterable, Tuple

from sqlmodel import Session, select

from shared.models.postgres import SubnetLookup
from tools.sqlmodel_helpers import resolve_sync_dsn, session_scope


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


def insert_subnet(session: Session, cidr: str, ip_start: str, ip_end: str) -> bool:
    existing = session.exec(select(SubnetLookup.id).where(SubnetLookup.cidr == cidr)).first()
    if existing:
        return False

    session.add(
        SubnetLookup(
            cidr=cidr,
            ip_start=ip_start,
            ip_end=ip_end,
            source="import_ip",
        )
    )
    return True


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
    postgres_dsn = resolve_sync_dsn(args.postgres_dsn)

    entries = list(load_entries(args.input))
    if not entries:
        print("[INFO] No IP addresses or CIDR ranges to import")
        return

    print(f"[INFO] Importing {len(entries)} subnet entries into PostgreSQL")

    with session_scope(dsn=postgres_dsn) as session:
        for entry in entries:
            cidr, ip_start, ip_end = parse_entry(entry)
            inserted = insert_subnet(session, cidr, ip_start, ip_end)
            session.commit()
            if inserted:
                print(f"INFO: Added subnet {cidr} ({ip_start} - {ip_end})")
            else:
                print(f"INFO: Subnet {cidr} already exists; skipping")

    print("[INFO] Subnet import completed")


if __name__ == "__main__":
    CLITool(main).run()
