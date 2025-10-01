#!/usr/bin/env python3

import pyqrcode
import multiprocessing
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import asyncpg
import click

from postgres_helpers import load_postgres_dsn, normalise_asyncpg_dsn


BATCH_SIZE = 100  # how many records each worker processes at once


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with PostgreSQL columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def create_postgres_pool(dsn: str) -> asyncpg.Pool:
    """Create a PostgreSQL connection pool."""
    return await asyncpg.create_pool(dsn)


async def retrieve_domains(
    pool: asyncpg.Pool, offset: int, limit: int
) -> List[Dict[str, Any]]:
    """Retrieve domains that don't have QR codes yet."""
    # Ensure the qrcode column exists
    await ensure_qrcode_column(pool)
    
    async with pool.acquire() as conn:
        try:
            query = """
                SELECT id, name
                FROM domains
                WHERE qrcode IS NULL
                  AND name ~ '^(([\\w]*\\.)?(?!(xn--)+)[\\w]*\\.[\\w]+)$'
                ORDER BY updated_at DESC
                OFFSET $1 LIMIT $2
            """
            rows = await conn.fetch(query, offset, limit)
            return [{'id': row['id'], 'name': row['name']} for row in rows]
        except Exception as e:
            print(f"ERROR: Failed to retrieve domains: {e}")
            return []


async def update_data(pool: asyncpg.Pool, domain_id: int, qrcode: str) -> None:
    """Update domain with QR code data."""
    async with pool.acquire() as conn:
        try:
            now = utcnow()
            result = await conn.execute(
                """
                UPDATE domains
                SET qrcode = $1, updated_at = $2
                WHERE id = $3
                """,
                qrcode, now, domain_id
            )
            
            if result == "UPDATE 1":
                print(f'INFO: added qrcode for domain ID {domain_id}')
        except Exception as e:
            print(f"ERROR: Failed to update domain {domain_id}: {e}")
            return


async def generate_qrcode(
    pool: asyncpg.Pool, domain_id: int, domain_name: str
) -> None:
    """Generate and store QR code for a domain."""
    url = pyqrcode.create(f'https://{domain_name}', encoding='utf-8')
    qrcode = url.png_as_base64_str(scale=5, quiet_zone=0)
    await update_data(pool, domain_id, qrcode)


async def worker_async(postgres_dsn: str, skip: int, limit: int) -> None:
    """Async worker to process QR code generation."""
    pool = await create_postgres_pool(postgres_dsn)
    
    try:
        domains = await retrieve_domains(pool, skip, limit)
        
        for domain in domains:
            await generate_qrcode(pool, domain['id'], domain['name'])
            
    except KeyboardInterrupt:
        return
    except Exception as e:
        print(f"ERROR: Worker failed: {e}")
        return
    finally:
        await pool.close()


def worker(postgres_dsn: str, skip: int, limit: int) -> None:
    """Synchronous wrapper for the async worker."""
    asyncio.run(worker_async(postgres_dsn, skip, limit))


async def ensure_qrcode_column(pool: asyncpg.Pool) -> None:
    """Ensure the qrcode column exists in the domains table."""
    async with pool.acquire() as conn:
        try:
            # Check if qrcode column exists
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'domains' AND column_name = 'qrcode'
                )
            """)
            
            if not exists:
                # Add the qrcode column
                await conn.execute("""
                    ALTER TABLE domains ADD COLUMN qrcode TEXT
                """)
                print("[INFO] Added qrcode column to domains table")
            else:
                print("[INFO] QR code column already exists")
        except Exception as e:
            print(f"[ERROR] Failed to ensure qrcode column: {e}")
            raise


async def get_domain_count(postgres_dsn: str) -> int:
    """Get the count of domains that need QR codes."""
    pool = await create_postgres_pool(postgres_dsn)
    try:
        # Ensure the qrcode column exists
        await ensure_qrcode_column(pool)
        
        async with pool.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*)
                FROM domains
                WHERE qrcode IS NULL
                  AND name ~ '^(([\\w]*\\.)?(?!(xn--)+)[\\w]*\\.[\\w]+)$'
            """)
            return count or 0
    finally:
        await pool.close()


@click.command()
@click.option(
    "--workers",
    type=int,
    default=4,
    help="Number of worker processes to run"
)
@click.option(
    "--postgres-dsn",
    type=str,
    envvar="POSTGRES_DSN",
    show_envvar=True,
    help="PostgreSQL DSN (or set POSTGRES_DSN env var)",
)
def main(workers: int, postgres_dsn: str | None):
    """QR code generation tool with PostgreSQL backend.
    
    Generates QR codes for domains that don't have them yet.
    """
    click.echo("[INFO] Starting QR code generator")
    click.echo(f"[INFO] Workers: {workers}")
    
    # Get total count of domains that need QR codes
    dsn = normalise_asyncpg_dsn(load_postgres_dsn(postgres_dsn))

    total_domains = asyncio.run(get_domain_count(dsn))
    if total_domains == 0:
        click.echo("[INFO] No domains need QR codes")
        return
    
    click.echo(f"[INFO] Found {total_domains} domains needing QR codes")
    
    # Calculate work distribution
    batch_size = max(1, total_domains // workers)
    
    jobs = []
    for i in range(workers):
        skip = i * batch_size
        limit = batch_size
        
        # Last worker gets any remaining domains
        if i == workers - 1:
            limit = max(0, total_domains - skip)
        
        if limit <= 0:
            break
            
        j = multiprocessing.Process(
            target=worker,
            args=(dsn, skip, limit)
        )
        jobs.append(j)
        j.start()
        click.echo(f"[INFO] Started worker {i+1} (skip={skip}, limit={limit})")

    for j in jobs:
        j.join()
        click.echo(f'[INFO] Worker finished with exitcode = {j.exitcode}')
    
    click.echo("[INFO] All workers finished")


if __name__ == "__main__":
    main()
