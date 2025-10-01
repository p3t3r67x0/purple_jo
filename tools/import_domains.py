#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Seed the PostgreSQL URLs table from a plaintext list."""

from __future__ import annotations

import argparse
import logging
from importlib import import_module
from pathlib import Path
from typing import Iterable

try:  # pragma: no cover - alias for direct script execution
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

bootstrap.setup()

from sqlmodel import Session, select

from shared.models.postgres import Url  # noqa: E402
from tools.sqlmodel_helpers import resolve_sync_dsn, session_scope  # noqa: E402


LOGGER = logging.getLogger(__name__)


class UrlImporter:
    """Import a set of URLs into the PostgreSQL-backed Url table."""

    def __init__(self, input_path: Path, postgres_dsn: str | None) -> None:
        self.input_path = input_path
        self.postgres_dsn = resolve_sync_dsn(postgres_dsn)

    @staticmethod
    def load_urls(path: Path) -> Iterable[str]:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                url = line.strip().lower()
                if not url:
                    continue
                yield url

    @staticmethod
    def normalise_url(url: str) -> str:
        if url.startswith("www."):
            return url[4:]
        return url

    @classmethod
    def build_parser(cls) -> argparse.ArgumentParser:
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

    @classmethod
    def from_args(cls) -> "UrlImporter":
        args = cls.build_parser().parse_args()
        return cls(input_path=args.input, postgres_dsn=args.postgres_dsn)

    @staticmethod
    def insert_url(session: Session, url: str) -> bool:
        existing = session.exec(select(Url.id).where(Url.url == url)).first()
        if existing:
            return False

        session.add(Url(url=url))
        return True

    def run(self) -> None:
        urls = list(self.load_urls(self.input_path))
        if not urls:
            LOGGER.info("No URLs to import from %s", self.input_path)
            return

        LOGGER.info("Importing %d URLs into PostgreSQL", len(urls))

        with session_scope(dsn=self.postgres_dsn) as session:
            for raw_url in urls:
                url = self.normalise_url(raw_url)
                inserted = self.insert_url(session, url)
                session.commit()
                if inserted:
                    LOGGER.info("Inserted URL %s", url)
                else:
                    LOGGER.info("URL %s already exists; skipping", url)

        LOGGER.info("URL import completed")


if __name__ == "__main__":
    UrlImporter.from_args().run()
