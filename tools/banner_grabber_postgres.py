#!/usr/bin/env python3
"""Collect SSH banners for domains stored in PostgreSQL (migrated from MongoDB)."""

from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import asyncpg
import click
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.db_postgres import get_session_factory
from app.models.postgres import Domain

DEFAULT_PORT = 22
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse environment variables from .env file."""
    env_vars = {}
    if not path.exists():
        return env_vars
    
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip().strip('"\'')
    return env_vars


def resolve_dsn() -> str:
    """Resolve PostgreSQL DSN from environment or .env file."""
    env_value = os.environ.get("POSTGRES_DSN")
    if env_value:
        dsn = env_value
    else:
        file_values = _parse_env_file(ENV_PATH)
        dsn = file_values.get("POSTGRES_DSN")

    if not dsn:
        raise RuntimeError(
            "POSTGRES_DSN must be set as an environment variable or in .env file"
        )

    # Convert SQLAlchemy DSN to asyncpg format
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://"):]
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://"):]
    return dsn


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with SQLModel columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def ensure_state_table(pool: asyncpg.Pool) -> None:
    """Ensure the banner_scan_state table exists."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS banner_scan_state (
                domain_id INTEGER PRIMARY KEY REFERENCES domains(id),
                banner_scan_started TIMESTAMP,
                banner_scan_failed TIMESTAMP,
                banner_scan_completed TIMESTAMP,
                banner TEXT,
                port INTEGER DEFAULT 22,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS ix_banner_scan_state_started 
            ON banner_scan_state(banner_scan_started);
            
            CREATE INDEX IF NOT EXISTS ix_banner_scan_state_failed 
            ON banner_scan_state(banner_scan_failed);
        """)


def _reset_failed_filter_clause(retry_failed: bool) -> str:
    """Generate the WHERE clause for filtering failed scans."""
    base_clause = """
        bss.banner_scan_started IS NULL 
        AND bss.banner_scan_completed IS NULL
    """
    
    if retry_failed:
        return base_clause
    else:
        return f"{base_clause} AND bss.banner_scan_failed IS NULL"


async def retrieve_batch(
    pool: asyncpg.Pool,
    batch_size: int,
    port: int,
    retry_failed: bool,
) -> list[dict[str, str | int]]:
    """Claim a batch of domains needing banner scans using SKIP LOCKED."""
    
    filter_clause = _reset_failed_filter_clause(retry_failed)
    
    # Step 1: Select and lock candidate domain IDs from domains table only
    select_query = f"""
        SELECT d.id
        FROM domains d
        LEFT JOIN banner_scan_state bss ON d.id = bss.domain_id
        WHERE {filter_clause}
        AND EXISTS (
            SELECT 1 FROM port_services ps 
            WHERE ps.domain_id = d.id AND ps.port = $1
        )
        ORDER BY d.updated_at DESC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    """
    
    now = utcnow()
    
    async with pool.acquire() as conn:
        # Step 1: Claim candidate domain IDs
        candidate_rows = await conn.fetch(select_query, port, batch_size)
        candidate_ids = [row["id"] for row in candidate_rows]
        if not candidate_ids:
            return []
        
        # Step 2: Insert/update banner_scan_state for claimed domains
        insert_query = f"""
            INSERT INTO banner_scan_state (domain_id, banner_scan_started, port)
            SELECT id, $2, $1
            FROM unnest($3::int[]) AS id
            ON CONFLICT (domain_id) DO UPDATE SET
                banner_scan_started = $2,
                banner_scan_failed = NULL
            RETURNING domain_id, (
                SELECT name FROM domains WHERE id = domain_id
            ) as domain_name
        """
        rows = await conn.fetch(insert_query, port, now, candidate_ids)
        return [
            {
                "domain_id": row["domain_id"],
                "domain": row["domain_name"],
                "port": port,
            }
            for row in rows
        ]


async def grab_banner(domain: str, port: int, timeout: int = 5) -> str | None:
    """Attempt to grab a banner from the given domain and port."""
    try:
        # Create socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        
        try:
            sock.connect((domain, port))
            # Send a simple request or just wait for banner
            if port == 22:  # SSH
                banner = sock.recv(1024).decode('utf-8', errors='ignore').strip()
            elif port == 21:  # FTP
                banner = sock.recv(1024).decode('utf-8', errors='ignore').strip()
            elif port == 25:  # SMTP
                banner = sock.recv(1024).decode('utf-8', errors='ignore').strip()
            else:
                # Generic banner grab
                banner = sock.recv(1024).decode('utf-8', errors='ignore').strip()
            
            return banner if banner else None
            
        finally:
            sock.close()
            
    except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError):
        return None


async def update_banner_success(
    pool: asyncpg.Pool, 
    domain_id: int, 
    banner: str
) -> None:
    """Mark a domain's banner scan as successful."""
    now = utcnow()
    
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE banner_scan_state 
            SET banner_scan_completed = $1, 
                banner = $2,
                updated_at = $1
            WHERE domain_id = $3
        """, now, banner, domain_id)
        
        # Also update the main domain record
        await conn.execute("""
            UPDATE domains 
            SET banner = $1, updated_at = $2 
            WHERE id = $3
        """, banner, now, domain_id)


async def update_banner_failure(pool: asyncpg.Pool, domain_id: int) -> None:
    """Mark a domain's banner scan as failed."""
    now = utcnow()
    
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE banner_scan_state 
            SET banner_scan_failed = $1,
                banner_scan_started = NULL,
                updated_at = $1
            WHERE domain_id = $2
        """, now, domain_id)


async def process_banner_batch(
    pool: asyncpg.Pool,
    domains: list[dict[str, str | int]],
    timeout: int
) -> tuple[int, int]:
    """Process a batch of domains for banner collection."""
    success_count = 0
    failure_count = 0
    
    for domain_info in domains:
        domain = domain_info["domain"]
        domain_id = domain_info["domain_id"] 
        port = domain_info["port"]
        
        print(f"INFO: Scanning {domain}:{port} for banner")
        
        banner = await grab_banner(domain, port, timeout)
        
        if banner:
            await update_banner_success(pool, domain_id, banner)
            print(f"INFO: Banner collected for {domain}: {banner[:50]}...")
            success_count += 1
        else:
            await update_banner_failure(pool, domain_id)
            print(f"INFO: No banner found for {domain}:{port}")
            failure_count += 1
    
    return success_count, failure_count


@click.command()
@click.option('--batch-size', default=50, help='Domains to process per batch')
@click.option('--port', default=DEFAULT_PORT, help='Port to scan for banners')
@click.option('--timeout', default=5, help='Socket timeout in seconds')
@click.option('--retry-failed', is_flag=True, help='Retry previously failed scans')
@click.option('--max-batches', default=0, help='Maximum batches to process (0 = unlimited)')
def main(
    batch_size: int, 
    port: int, 
    timeout: int, 
    retry_failed: bool,
    max_batches: int
):
    """Collect SSH/service banners for domains in PostgreSQL."""
    
    async def run_banner_collection():
        dsn = resolve_dsn()
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        
        try:
            await ensure_state_table(pool)
            
            total_success = 0
            total_failure = 0
            batch_count = 0
            
            print(f"Starting banner collection on port {port}")
            print(f"Batch size: {batch_size}, Timeout: {timeout}s")
            print(f"Retry failed: {retry_failed}")
            
            while max_batches == 0 or batch_count < max_batches:
                # Get batch of domains to scan
                domains = await retrieve_batch(pool, batch_size, port, retry_failed)
                
                if not domains:
                    print("No more domains to scan")
                    break
                
                print(f"Processing batch {batch_count + 1} with {len(domains)} domains")
                
                # Process the batch
                success, failure = await process_banner_batch(pool, domains, timeout)
                
                total_success += success
                total_failure += failure
                batch_count += 1
                
                print(f"Batch {batch_count} completed: {success} success, {failure} failed")
                
                # Small delay between batches
                await asyncio.sleep(1)
            
            print(f"\nBanner collection completed:")
            print(f"Total batches: {batch_count}")
            print(f"Total success: {total_success}")
            print(f"Total failures: {total_failure}")
            
        finally:
            await pool.close()
    
    asyncio.run(run_banner_collection())


if __name__ == '__main__':
    main()