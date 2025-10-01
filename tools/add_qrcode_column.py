#!/usr/bin/env python3

"""Utility script to add the qrcode column to the domains table."""

import argparse
import asyncio

import asyncpg

from postgres_helpers import load_postgres_dsn, normalise_asyncpg_dsn


async def add_qrcode_column(postgres_dsn: str) -> None:
    """Add the qrcode column to the domains table if missing."""
    conn = await asyncpg.connect(postgres_dsn)

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
            print("[SUCCESS] Added qrcode column to domains table")
        else:
            print("[INFO] QR code column already exists")

    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"[ERROR] Failed to add qrcode column: {exc}")
        raise
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add qrcode column")
    parser.add_argument(
        "--postgres-dsn",
        default=None,
        help="PostgreSQL DSN",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dsn = normalise_asyncpg_dsn(load_postgres_dsn(args.postgres_dsn))
    asyncio.run(add_qrcode_column(dsn))


if __name__ == "__main__":
    main()
