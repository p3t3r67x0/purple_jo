#!/usr/bin/env python3

import asyncio
from datetime import datetime, timezone
from typing import List

import asyncpg
import click

from postgres_helpers import load_postgres_dsn, normalise_asyncpg_dsn


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with PostgreSQL columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def create_postgres_pool(dsn: str) -> asyncpg.Pool:
    """Create a PostgreSQL connection pool."""
    return await asyncpg.create_pool(dsn)


async def check_asn_table(pool: asyncpg.Pool) -> None:
    """Check if the ASN table exists and is properly migrated."""
    async with pool.acquire() as conn:
        try:
            result = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'asn_records'
                )
            """)
            if not result:
                raise RuntimeError(
                    "ASN table not found. Please run 'alembic upgrade head' "
                    "to create the required database schema."
                )
        except Exception as e:
            print(f"[ERROR] Failed to check ASN table: {e}")
            raise


async def add_asn_record(pool: asyncpg.Pool, cidr: str, asn: str) -> None:
    """Add a CIDR/ASN record to the database."""
    async with pool.acquire() as conn:
        try:
            now = utcnow()
            query = (
                "INSERT INTO asn_records (cidr, asn, created_at) "
                "VALUES ($1, $2, $3)"
            )
            result = await conn.execute(query, cidr, asn, now)
            if result == "INSERT 0 1":
                print(f'INFO: CIDR {cidr} -> ASN {asn} was added successfully')
        except asyncpg.UniqueViolationError:
            # Record already exists, skip silently
            pass
        except Exception as e:
            print(f"ERROR: Failed to add CIDR {cidr} -> ASN {asn}: {e}")


def load_asn_file(filename: str) -> List[tuple]:
    """Load CIDR/ASN pairs from a text file."""
    records = []
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith(';'):
                continue
            
            # Parse CIDR and ASN (format: "1.0.0.0/24      13335")
            parts = line.split()
            if len(parts) >= 2:
                cidr = parts[0]
                asn = parts[1]
                records.append((cidr, asn))
    
    return records


async def process_asns(postgres_dsn: str, input_file: str) -> None:
    """Process ASN file and insert records into PostgreSQL."""
    pool = await create_postgres_pool(postgres_dsn)

    try:
        # Check if the ASN table exists
        await check_asn_table(pool)

        # Load CIDR/ASN pairs from file
        records = load_asn_file(input_file)
        click.echo(f"[INFO] Loaded {len(records)} CIDR/ASN records from "
                   f"{input_file}")

        # Process each record
        for cidr, asn in records:
            click.echo(f'INFO: Processing {cidr} -> ASN {asn}')
            await add_asn_record(pool, cidr, asn)

        click.echo("[INFO] ASN processing completed")

    finally:
        await pool.close()


@click.command()
@click.option(
    "--input",
    type=click.Path(exists=True, readable=True),
    required=True,
    help="Input file containing CIDR/ASN pairs (e.g., '1.0.0.0/24 13335')"
)
@click.option(
    "--postgres-dsn",
    type=str,
    envvar="POSTGRES_DSN",
    show_envvar=True,
    help="PostgreSQL DSN (or set POSTGRES_DSN env var)",
)
def main(input: str, postgres_dsn: str | None):
    """ASN insertion tool with PostgreSQL backend.

    Loads CIDR/ASN pairs from a text file and inserts them into PostgreSQL.
    Each line should contain a CIDR block and ASN number separated by
    whitespace. Lines starting with ';' are treated as comments and skipped.
    """
    click.echo("[INFO] Starting ASN insertion")
    click.echo(f"[INFO] Input file: {input}")

    dsn = normalise_asyncpg_dsn(load_postgres_dsn(postgres_dsn))

    asyncio.run(process_asns(dsn, input))


if __name__ == '__main__':
    main()
