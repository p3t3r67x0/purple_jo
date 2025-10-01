#!/usr/bin/env python3
try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool
import sys
import json
import contextlib
import multiprocessing
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import List, Dict, Any

import asyncpg
import asyncio
import click

from async_sqlmodel_helpers import asyncpg_pool_dsn

def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with PostgreSQL columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def load_ports(filename):
    try:
        with open(filename, "r") as f:
            ports = [line.strip() for line in f if line.strip()]
        return ",".join(ports)
    except IOError as e:
        print(f"ERROR: cannot read ports file {filename}: {e}")
        sys.exit(1)


async def create_postgres_pool(dsn: str) -> asyncpg.Pool:
    """Create a PostgreSQL connection pool."""
    return await asyncpg.create_pool(asyncpg_pool_dsn(dsn))


async def claim_ips(pool: asyncpg.Pool, batch_size: int) -> List[str]:
    """Atomically claim a batch of IPs for scanning."""
    async with pool.acquire() as conn:
        # Create scanning state table if not exists
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS masscan_state (
                domain_id INTEGER PRIMARY KEY REFERENCES domains(id),
                claimed_at TIMESTAMP DEFAULT NOW(),
                scan_completed TIMESTAMP
            )
        """)
        
        # Step 1: Find and claim unclaimed domains with A records
        claim_query = """
            WITH candidates AS (
                SELECT DISTINCT d.id as domain_id
                FROM domains d
                JOIN a_records ar ON d.id = ar.domain_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM masscan_state ms WHERE ms.domain_id = d.id
                )
                ORDER BY d.id
                LIMIT $1
            )
            INSERT INTO masscan_state (domain_id)
            SELECT domain_id FROM candidates
            ON CONFLICT (domain_id) DO NOTHING
            RETURNING domain_id
        """
        
        claimed_rows = await conn.fetch(claim_query, batch_size)
        if not claimed_rows:
            return []
        
        claimed_domain_ids = [row['domain_id'] for row in claimed_rows]
        
        # Step 2: Get IP addresses for claimed domains
        ip_query = """
            SELECT array_agg(DISTINCT ar.ip_address) as ip_addresses
            FROM a_records ar
            WHERE ar.domain_id = ANY($1)
        """
        
        ip_rows = await conn.fetch(ip_query, claimed_domain_ids)
        if not ip_rows or not ip_rows[0]['ip_addresses']:
            return []
        
        return ip_rows[0]['ip_addresses']


def run_masscan(ips, ports, rate=1000):
    """Run masscan and return parsed results."""
    if not ips:
        return []

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        out_file = tmpfile.name

    cmd = [
        "masscan",
        "-p", ports,
        "--rate", str(rate),
        "--open",
        "--banners",
        "-oJ", out_file,
    ] + ips

    print(f"[INFO] Running: {' '.join(cmd)}")

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[-] masscan failed: {e}")
        return []

    try:
        with open(out_file, "r") as f:
            content = f.read()
    except Exception as e:
        print(f"[-] Failed to read masscan output: {e}")
        return []
    finally:
        with contextlib.suppress(Exception):
            os.remove(out_file)

    if not content.strip():
        return []

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        # Fallback to line-by-line JSON (masscan sometimes emits NDJSON style)
        results = []
        for raw in content.splitlines():
            line = raw.strip().rstrip(",")
            if not line or line in {"[", "]"}:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as e:
                preview = line[:80] + ("..." if len(line) > 80 else "")
                print(
                    f"[-] Failed to parse masscan JSON line: {e} -> {preview}"
                )
                continue

            if "ip" in payload and payload.get("ports"):
                results.append(payload)
            elif "results" in payload:
                results.extend(payload.get("results", []))
        return results

    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict) and "results" in parsed:
        return parsed.get("results", [])
    return []


async def update_data(
    pool: asyncpg.Pool,
    scan_results: List[Dict[str, Any]],
    now: datetime
) -> None:
    """Bulk update port_services table with scan results."""
    if not scan_results:
        return
    
    async with pool.acquire() as conn:
        port_service_inserts = []
        updated_domains = set()
        
        for result in scan_results:
            ip = result.get("ip")
            ports_info = result.get("ports", [])
            if not ip or not ports_info:
                continue

            # Find domains that have this IP address
            domain_rows = await conn.fetch("""
                SELECT d.id as domain_id
                FROM domains d
                JOIN a_records ar ON d.id = ar.domain_id
                WHERE ar.ip_address = $1
            """, ip)
            
            for domain_row in domain_rows:
                domain_id = domain_row['domain_id']
                updated_domains.add(domain_id)
                
                for port_info in ports_info:
                    port = port_info.get("port")
                    status = port_info.get("status", "open")
                    
                    # Handle service field - it can be a string or dict
                    service_info = port_info.get("service", "")
                    if isinstance(service_info, dict):
                        # Extract service name from dict, fallback to empty
                        service = service_info.get("name", "")
                        # Ensure service is a string
                        if not isinstance(service, str):
                            service = str(service) if service else ""
                    elif isinstance(service_info, str):
                        service = service_info
                    else:
                        # Convert any other type to string
                        service = str(service_info) if service_info else ""
                    
                    if port:
                        port_service_inserts.append((
                            domain_id, port, status, service, now
                        ))

        if port_service_inserts:
            # First, delete existing port services for these domains
            # to avoid duplicates (rescans might have different results)
            domain_ids_for_cleanup = list(updated_domains)
            if domain_ids_for_cleanup:
                await conn.execute("""
                    DELETE FROM port_services
                    WHERE domain_id = ANY($1)
                """, domain_ids_for_cleanup)
            
            # Insert new port services
            await conn.executemany("""
                INSERT INTO port_services (
                    domain_id, port, status, service, updated_at
                )
                VALUES ($1, $2, $3, $4, $5)
            """, port_service_inserts)
            
            print(f"[INFO] Inserted/updated {len(port_service_inserts)} "
                  f"port services for {len(updated_domains)} domains")

        # Mark domains as scan completed
        if updated_domains:
            await conn.execute("""
                UPDATE masscan_state
                SET scan_completed = $1
                WHERE domain_id = ANY($2)
            """, now, list(updated_domains))
        
        # Update domains' updated_at timestamp
        if updated_domains:
            await conn.execute("""
                UPDATE domains
                SET updated_at = $1
                WHERE id = ANY($2)
            """, now, list(updated_domains))


async def worker_async(
    postgres_dsn: str, ports: str, rate: int, batch_size: int
):
    """Async worker that scans IPs and updates PostgreSQL."""
    pool = await create_postgres_pool(postgres_dsn)
    
    try:
        while True:
            ips = await claim_ips(pool, batch_size)
            if not ips:
                print("[INFO] No more IPs to scan")
                break

            print(f"[INFO] Scanning {len(ips)} IPs")
            scan_results = run_masscan(ips, ports, rate=rate)
            await update_data(pool, scan_results, utcnow())
    finally:
        await pool.close()


def worker(postgres_dsn: str, ports: str, rate: int, batch_size: int):
    """Synchronous wrapper for the async worker."""
    asyncio.run(worker_async(postgres_dsn, ports, rate, batch_size))


@click.command()
@click.option(
    "--workers",
    type=int,
    required=True,
    help="Number of worker processes to run"
)
@click.option(
    "--input",
    "input_file",
    type=click.Path(exists=True, readable=True),
    required=True,
    help="Path to ports file"
)
@click.option(
    "--batch",
    type=int,
    default=500,
    help="Number of IPs per worker batch"
)
@click.option(
    "--rate",
    type=int,
    default=5000,
    help="Masscan scan rate (packets per second)"
)
@click.option(
    "--postgres-dsn",
    required=True,
    type=str,
    help="PostgreSQL DSN",
)
def main(
    workers: int,
    input_file: str,
    batch: int,
    rate: int,
    postgres_dsn: str,
):
    """Masscan scanner tool with PostgreSQL backend.
    
    Scans domains from the database using masscan and stores results.
    """
    ports = load_ports(input_file)

    click.echo(f"[INFO] Ports loaded: {ports}")
    click.echo(
        f"[INFO] Starting {workers} processes, "
        f"batch size {batch}, rate {rate}"
    )

    with multiprocessing.Pool(processes=workers) as pool:
        pool.starmap(
            worker,
            [(postgres_dsn, ports, rate, batch)] * workers
        )

    click.echo("[INFO] All workers finished")


if __name__ == "__main__":
    CLITool(main).run()
