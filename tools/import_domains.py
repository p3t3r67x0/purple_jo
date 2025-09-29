#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Seed the PostgreSQL URLs table from a plaintext list."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

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
    """Resolve a PostgreSQL DSN from CLI, environment, or .env."""

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


def load_urls(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            url = line.strip().lower()
            if not url:
                continue
            yield url


def normalise_url(url: str) -> str:
    if url.startswith("www."):
        return url[4:]
    return url


def insert_url(conn: psycopg.Connection, url: str) -> bool:
    now = utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO urls (url, created_at)
            VALUES (%s, %s)
            ON CONFLICT (url) DO NOTHING
            """,
            (url, now),
        )
        inserted = cur.rowcount == 1
    conn.commit()
    return inserted


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import URLs into PostgreSQL")
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Plaintext file containing one URL per line",
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

    urls = list(load_urls(args.input))
    if not urls:
        print("[INFO] No URLs to import")
        return

    print(f"[INFO] Importing {len(urls)} URLs into PostgreSQL")

    with psycopg.connect(postgres_dsn) as conn:
        for raw_url in urls:
            url = normalise_url(raw_url)
            inserted = insert_url(conn, url)
            if inserted:
                print(f"INFO: inserted URL {url}")
            else:
                print(f"INFO: URL {url} already exists; skipping")

    print("[INFO] URL import completed")


if __name__ == "__main__":
    main()
