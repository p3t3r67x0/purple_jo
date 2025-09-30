#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Seed the PostgreSQL URLs table from a plaintext list."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from sqlmodel import Session, select

from app.models.postgres import Url
from tools.sqlmodel_helpers import resolve_sync_dsn, session_scope


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


def insert_url(session: Session, url: str) -> bool:
    existing = session.exec(select(Url.id).where(Url.url == url)).first()
    if existing:
        return False

    session.add(Url(url=url))
    return True


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
    postgres_dsn = resolve_sync_dsn(args.postgres_dsn)

    urls = list(load_urls(args.input))
    if not urls:
        print("[INFO] No URLs to import")
        return

    print(f"[INFO] Importing {len(urls)} URLs into PostgreSQL")

    with session_scope(dsn=postgres_dsn) as session:
        for raw_url in urls:
            url = normalise_url(raw_url)
            inserted = insert_url(session, url)
            session.commit()
            if inserted:
                print(f"INFO: inserted URL {url}")
            else:
                print(f"INFO: URL {url} already exists; skipping")

    print("[INFO] URL import completed")


if __name__ == "__main__":
    main()
