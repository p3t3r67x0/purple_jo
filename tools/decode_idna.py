#!/usr/bin/env python3

import asyncio
import idna
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import asyncpg
import click

from idna.core import IDNAError


ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_file(path: Path) -> dict:
    """Parse a .env file and return key-value pairs."""
    env_vars = {}
    if not path.exists():
        return env_vars

    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip().strip('"\'')
    return env_vars


def resolve_dsn() -> str:
    """Resolve PostgreSQL DSN from environment or .env file."""
    # First try environment variables
    if "POSTGRES_DSN" in os.environ:
        dsn = os.environ["POSTGRES_DSN"]
    else:
        # Try to load from .env file
        env_vars = _parse_env_file(ENV_PATH)
        dsn = env_vars.get("POSTGRES_DSN")

        if not dsn:
            raise ValueError(
                "POSTGRES_DSN not found in environment or .env file"
            )

    # Convert SQLAlchemy DSN to asyncpg format
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://"):]
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://"):]
    return dsn


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with PostgreSQL columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def create_postgres_pool(dsn: str) -> asyncpg.Pool:
    """Create a PostgreSQL connection pool."""
    return await asyncpg.create_pool(dsn)


async def retrieve_idna_domains(pool: asyncpg.Pool) -> List[Tuple[int, str]]:
    """Retrieve domains containing IDNA encoding (xn--) from PostgreSQL."""
    async with pool.acquire() as conn:
        try:
            # Find domains that contain IDNA encoding patterns
            query = """
                SELECT id, name
                FROM domains
                WHERE name ~ '(^|[.])(xn--)[a-z0-9]+'
                ORDER BY id
            """
            rows = await conn.fetch(query)
            return [(row['id'], row['name']) for row in rows]
        except Exception as e:
            print(f"[ERROR] Failed to retrieve IDNA domains: {e}")
            return []


async def update_domain_name(pool: asyncpg.Pool, domain_id: int,
                             original_name: str, decoded_name: str) -> None:
    """Update domain name with decoded IDNA version."""
    async with pool.acquire() as conn:
        try:
            now = utcnow()
            result = await conn.execute("""
                UPDATE domains
                SET name = $1, updated_at = $2
                WHERE id = $3
            """, decoded_name, now, domain_id)

            if result == "UPDATE 1":
                msg = f'INFO: Updated domain {original_name} -> {decoded_name}'
                print(msg)
            else:
                print(f'WARNING: Failed to update domain ID {domain_id}')
        except Exception as e:
            print(f"ERROR: Failed to update domain {original_name}: {e}")


async def process_idna_domains(postgres_dsn: str) -> None:
    """Process IDNA domains and decode them."""
    pool = await create_postgres_pool(postgres_dsn)

    try:
        # Retrieve domains with IDNA encoding
        domains = await retrieve_idna_domains(pool)

        if not domains:
            click.echo("[INFO] No IDNA-encoded domains found")
            return

        click.echo(f"[INFO] Found {len(domains)} IDNA-encoded domains")

        decoded_count = 0
        for domain_id, idna_domain in domains:
            try:
                # Attempt to decode IDNA domain
                decoded_domain = idna.decode(idna_domain)

                # Only update if decoding actually changed the domain
                if decoded_domain and idna_domain != decoded_domain:
                    await update_domain_name(pool, domain_id,
                                             idna_domain, decoded_domain)
                    decoded_count += 1

            except (UnicodeError, IDNAError) as e:
                print(f"WARNING: Failed to decode {idna_domain}: {e}")
                continue

        click.echo(f"[INFO] Successfully decoded {decoded_count} domains")

    finally:
        await pool.close()


@click.command()
def main():
    """IDNA decoder tool with PostgreSQL backend.

    Finds domains with IDNA encoding (containing 'xn--') and decodes them
    to their Unicode representation, updating the database accordingly.
    """
    postgres_dsn = resolve_dsn()

    click.echo("[INFO] Starting IDNA domain decoding")

    asyncio.run(process_idna_domains(postgres_dsn))


if __name__ == '__main__':
    main()
