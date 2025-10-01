#!/usr/bin/env python3
try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

import asyncio
import multiprocessing
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ipwhois.asn import IPASN
from ipwhois.net import Net

import asyncpg
import click

from async_sqlmodel_helpers import asyncpg_pool_dsn


BATCH_SIZE = 100  # how many records each worker processes at once


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with PostgreSQL columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def create_postgres_pool(dsn: str) -> asyncpg.Pool:
    """Create a PostgreSQL connection pool."""
    return await asyncpg.create_pool(asyncpg_pool_dsn(dsn))


def get_whois(ip: str) -> Optional[Dict[str, Any]]:
    """Fetch WHOIS information for an IP address."""
    try:
        return IPASN(Net(ip)).lookup(retry_count=0, asn_methods=["whois"])
    except Exception:
        return None


async def initialize_whois_state_table(pool: asyncpg.Pool) -> None:
    """Initialize the whois_state table if it doesn't exist."""
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS whois_state (
                    domain_id INTEGER PRIMARY KEY REFERENCES domains(id),
                    claimed_at TIMESTAMP DEFAULT NOW(),
                    completed_at TIMESTAMP
                )
            """)
        except Exception as e:
            # If table creation fails (e.g., due to race condition),
            # check if table exists and continue if it does
            try:
                await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'whois_state'
                    )
                """)
                # Table exists, continue
                pass
            except Exception:
                # Re-raise the original error if table doesn't exist
                raise e


async def claim_domains(
    pool: asyncpg.Pool, batch_size: int
) -> List[Dict[str, Any]]:
    """Claim domains that need WHOIS lookups and return their data."""
    async with pool.acquire() as conn:
        # First, find candidate domains and get their data
        candidates_query = """
            SELECT DISTINCT d.id as domain_id, ar.ip_address
            FROM domains d
            JOIN a_records ar ON d.id = ar.domain_id
            WHERE NOT EXISTS (
                SELECT 1 FROM whois_records wr WHERE wr.domain_id = d.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM whois_state ws WHERE ws.domain_id = d.id
                AND ws.completed_at IS NULL
            )
            ORDER BY d.id
            LIMIT $1
        """
        
        candidates = await conn.fetch(candidates_query, batch_size)
        if not candidates:
            return []
        
        # Claim the domains by inserting into whois_state
        domain_ids = [row['domain_id'] for row in candidates]
        await conn.executemany(
            "INSERT INTO whois_state (domain_id) VALUES ($1) "
            "ON CONFLICT DO NOTHING",
            [(domain_id,) for domain_id in domain_ids]
        )
        
        # Return the candidate data as list of dictionaries
        return [
            {
                'domain_id': row['domain_id'],
                'ip_address': row['ip_address']
            }
            for row in candidates
        ]


async def update_whois_data(
    pool: asyncpg.Pool,
    domain_updates: List[Dict[str, Any]]
) -> None:
    """Update domains with WHOIS information."""
    if not domain_updates:
        return
    
    now = utcnow()
    async with pool.acquire() as conn:
        whois_inserts = []
        completed_domain_ids = []
        
        for update in domain_updates:
            domain_id = update['domain_id']
            whois_data = update.get('whois_data')
            
            completed_domain_ids.append(domain_id)
            
            if whois_data:
                # Extract WHOIS fields
                asn = whois_data.get('asn')
                asn_description = (whois_data.get('asn_description') or '')[:255]
                asn_country_code = (whois_data.get('asn_country_code') or '')[:8]
                asn_registry = (whois_data.get('asn_registry') or '')[:255]
                asn_cidr = (whois_data.get('asn_cidr') or '')[:64]
                
                whois_inserts.append((
                    domain_id, asn, asn_description, asn_country_code,
                    asn_registry, asn_cidr, now
                ))
        
        # Insert WHOIS records
        if whois_inserts:
            await conn.executemany("""
                INSERT INTO whois_records (
                    domain_id, asn, asn_description, asn_country_code,
                    asn_registry, asn_cidr, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (domain_id) DO UPDATE SET
                    asn = EXCLUDED.asn,
                    asn_description = EXCLUDED.asn_description,
                    asn_country_code = EXCLUDED.asn_country_code,
                    asn_registry = EXCLUDED.asn_registry,
                    asn_cidr = EXCLUDED.asn_cidr,
                    updated_at = EXCLUDED.updated_at
            """, whois_inserts)
        
        # Mark domains as completed
        if completed_domain_ids:
            await conn.execute("""
                UPDATE whois_state
                SET completed_at = $1
                WHERE domain_id = ANY($2)
            """, now, completed_domain_ids)


async def worker_async(postgres_dsn: str, batch_size: int) -> None:
    """Async worker to process WHOIS lookups."""
    pool = await create_postgres_pool(postgres_dsn)
    
    try:
        while True:
            # Claim a batch of domains to process
            domain_data = await claim_domains(pool, batch_size)
            if not domain_data:
                break
            
            # Process WHOIS lookups for claimed domains
            domain_updates = []
            for domain_info in domain_data:
                domain_id = domain_info['domain_id']
                ip_address = domain_info['ip_address']
                
                print(
                    f"[INFO] Fetching WHOIS for {ip_address} "
                    f"(domain {domain_id})"
                )
                whois_data = get_whois(ip_address)
                
                domain_updates.append({
                    'domain_id': domain_id,
                    'whois_data': whois_data
                })
            
            # Update database with WHOIS results
            await update_whois_data(pool, domain_updates)
            
    finally:
        await pool.close()


def worker(postgres_dsn: str, batch_size: int) -> None:
    """Synchronous wrapper for the async worker."""
    asyncio.run(worker_async(postgres_dsn, batch_size))


async def initialize_database(postgres_dsn: str) -> None:
    """Initialize the database schema before starting workers."""
    pool = await create_postgres_pool(postgres_dsn)
    try:
        await initialize_whois_state_table(pool)
        click.echo("[INFO] Database initialized successfully")
    finally:
        await pool.close()


@click.command()
@click.option(
    "--postgres-dsn",
    type=str,
    required=True,
    help="PostgreSQL DSN",
)
@click.option(
    "--workers",
    type=int,
    default=4,
    help="Number of worker processes to run"
)
@click.option(
    "--batch-size",
    type=int,
    default=BATCH_SIZE,
    help="Number of domains per worker batch"
)
def main(postgres_dsn: str, workers: int, batch_size: int):
    """WHOIS extraction tool with PostgreSQL backend.
    
    Fetches WHOIS information for domains with A records.
    """
    click.echo("[INFO] Starting WHOIS fetcher")
    click.echo(f"[INFO] Workers: {workers}, Batch size: {batch_size}")
    
    # Initialize database schema first
    asyncio.run(initialize_database(postgres_dsn))
    
    with multiprocessing.Pool(processes=workers) as pool:
        pool.starmap(
            worker,
            [(postgres_dsn, batch_size)] * workers
        )
    
    click.echo("[INFO] All workers finished")


if __name__ == "__main__":
    CLITool(main).run()
