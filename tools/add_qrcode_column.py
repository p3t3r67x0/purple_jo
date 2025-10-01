#!/usr/bin/env python3

"""Utility script to add the qrcode column to the domains table."""

from __future__ import annotations

import argparse
import asyncio
import logging

import asyncpg

from tools.async_sqlmodel_helpers import asyncpg_pool_dsn


LOGGER = logging.getLogger(__name__)


class QRCodeColumnAdder:
    """Ensure the domains table includes a ``qrcode`` column."""

    def __init__(self, postgres_dsn: str) -> None:
        self.postgres_dsn = postgres_dsn

    @classmethod
    def build_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Add qrcode column")
        parser.add_argument(
            "--postgres-dsn",
            required=True,
            help="PostgreSQL DSN",
        )
        return parser

    @classmethod
    def from_args(cls) -> "QRCodeColumnAdder":
        args = cls.build_parser().parse_args()
        return cls(postgres_dsn=args.postgres_dsn)

    async def add_column(self) -> None:
        conn = await asyncpg.connect(asyncpg_pool_dsn(self.postgres_dsn))

        try:
            exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'domains' AND column_name = 'qrcode'
                )
                """
            )

            if not exists:
                await conn.execute("ALTER TABLE domains ADD COLUMN qrcode TEXT")
                LOGGER.info("Added qrcode column to domains table")
            else:
                LOGGER.info("QR code column already exists")

        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to add qrcode column: %s", exc)
            raise
        finally:
            await conn.close()

    def run(self) -> None:
        asyncio.run(self.add_column())


if __name__ == "__main__":
    QRCodeColumnAdder.from_args().run()
