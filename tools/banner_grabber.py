#!/usr/bin/env python3

"""Collect SSH banners for domains stored in PostgreSQL."""

from __future__ import annotations

import asyncio
import socket
import time
from datetime import datetime, timezone
from typing import List

import asyncpg
import click

from async_sqlmodel_helpers import asyncpg_pool_dsn

DEFAULT_PORT = 22
def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with SQLModel columns."""

    return datetime.now(timezone.utc).replace(tzinfo=None)


async def ensure_state_table(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS banner_scan_state (
                domain_id INTEGER PRIMARY KEY
                    REFERENCES domains(id) ON DELETE CASCADE,
                in_progress BOOLEAN NOT NULL DEFAULT FALSE,
                last_attempted_at TIMESTAMP WITHOUT TIME ZONE,
                last_failed_at TIMESTAMP WITHOUT TIME ZONE,
                failure_reason TEXT
            )
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_banner_scan_state_last_failed
            ON banner_scan_state (last_failed_at)
            """
        )


def _reset_failed_filter_clause(retry_failed: bool) -> str:
    if retry_failed:
        return "TRUE"
    return "s.last_failed_at IS NULL"


async def retrieve_batch(
    pool: asyncpg.Pool,
    batch_size: int,
    port: int,
    retry_failed: bool,
) -> list[dict[str, str | int]]:
    """Claim a batch of domains needing banner scans using SKIP LOCKED."""

    async with pool.acquire() as conn:
        async with conn.transaction():
            failure_filter = _reset_failed_filter_clause(retry_failed)
            selections: List[asyncpg.Record] = await conn.fetch(
                f"""
                SELECT d.id, d.name
                FROM domains AS d
                JOIN port_services AS ps
                    ON ps.domain_id = d.id AND ps.port = $1
                LEFT JOIN banner_scan_state AS s
                    ON s.domain_id = d.id
                WHERE d.banner IS NULL
                  AND COALESCE(s.in_progress, FALSE) = FALSE
                  AND ({failure_filter})
                  AND EXISTS (
                      SELECT 1 FROM a_records ar WHERE ar.domain_id = d.id
                  )
                ORDER BY d.updated_at DESC
                LIMIT $2
                FOR UPDATE OF d SKIP LOCKED
                """,
                port,
                batch_size,
            )

            if not selections:
                return []

            # Ensure unique IDs to avoid CardinalityViolationError
            ids = list(set(int(row["id"]) for row in selections))
            now = utcnow()

            await conn.execute(
                """
                INSERT INTO banner_scan_state (domain_id, in_progress, last_attempted_at)
                SELECT id, TRUE, $1
                FROM unnest($2::int4[]) AS ids(id)
                ON CONFLICT (domain_id)
                DO UPDATE SET
                    in_progress = EXCLUDED.in_progress,
                    last_attempted_at = EXCLUDED.last_attempted_at
                """,
                now,
                ids,
            )

            ip_rows = await conn.fetch(
                """
                SELECT ar.domain_id AS id, ar.ip_address
                FROM a_records AS ar
                WHERE ar.domain_id = ANY($1::int4[])
                ORDER BY ar.domain_id, ar.updated_at DESC, ar.id ASC
                """,
                ids,
            )

    ip_map: dict[int, str] = {}
    for row in ip_rows:
        domain_id = int(row["id"])
        ip_address = row["ip_address"]
        if domain_id not in ip_map and ip_address:
            ip_map[domain_id] = ip_address

    batch: list[dict[str, str | int]] = []
    for row in selections:
        ip = ip_map.get(int(row["id"]))
        if not ip:
            continue
        batch.append({"id": int(row["id"]), "domain": row["name"], "ip": ip})

    return batch


def grab_banner(ip: str, port: int, timeout: float) -> str | None:
    """Attempt to read an SSH banner from ``ip:port`` with optimizations."""
    sock = None
    try:
        # Create socket with optimizations
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(timeout)

        # Connect and read banner
        sock.connect((ip, port))

        # Read with a reasonable buffer size
        banner = sock.recv(4096)  # Increased buffer for larger banners

        if not banner:
            return None

        # Clean the banner: remove null bytes and non-printable characters
        cleaned = banner.replace(b'\x00', b'').strip()
        if not cleaned:
            return None

        decoded = cleaned.decode("utf-8", errors="ignore").strip()
        if not decoded:
            return None

        # Further clean: remove control characters except newlines/tabs
        decoded = ''.join(char for char in decoded
                          if char.isprintable() or char in '\n\r\t').strip()

        return decoded if decoded else None

    except socket.timeout:
        return None  # Silent timeout - very common
    except (ConnectionRefusedError, OSError):
        return None  # Silent connection errors - also common
    except Exception as exc:
        print(f"ERROR: unexpected error {ip}:{port} -> {exc}")
        return None
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass  # Ignore close errors


async def _finalize_state_with_conn(
    conn: asyncpg.Connection,
    domain_id: int,
    *,
    failed: bool,
    failure_reason: str | None,
) -> None:
    now = utcnow()
    if failed:
        trimmed = (failure_reason or "")[:500]
        await conn.execute(
            """
            INSERT INTO banner_scan_state (
                domain_id,
                in_progress,
                last_attempted_at,
                last_failed_at,
                failure_reason
            )
            VALUES ($1, FALSE, $2, $3, $4)
            ON CONFLICT (domain_id)
            DO UPDATE SET
                in_progress = EXCLUDED.in_progress,
                last_attempted_at = EXCLUDED.last_attempted_at,
                last_failed_at = EXCLUDED.last_failed_at,
                failure_reason = EXCLUDED.failure_reason
            """,
            domain_id,
            now,
            now,
            trimmed,
        )
    else:
        await conn.execute(
            """
            INSERT INTO banner_scan_state (
                domain_id,
                in_progress,
                last_attempted_at,
                last_failed_at,
                failure_reason
            )
            VALUES ($1, FALSE, $2, NULL, NULL)
            ON CONFLICT (domain_id)
            DO UPDATE SET
                in_progress = EXCLUDED.in_progress,
                last_attempted_at = EXCLUDED.last_attempted_at,
                last_failed_at = NULL,
                failure_reason = NULL
            """,
            domain_id,
            now,
        )


async def persist_success(
    pool: asyncpg.Pool,
    *,
    domain_id: int,
    banner: str,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            now = utcnow()
            await conn.execute(
                """
                UPDATE domains
                SET banner = $1,
                    updated_at = $2
                WHERE id = $3
                """,
                banner,
                now,
                domain_id,
            )
            await _finalize_state_with_conn(
                conn, domain_id, failed=False, failure_reason=None
            )


async def persist_failure(
    pool: asyncpg.Pool,
    *,
    domain_id: int,
    error: str,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _finalize_state_with_conn(
                conn, domain_id, failed=True, failure_reason=error
            )


async def persist_batch_results(
    pool: asyncpg.Pool,
    successes: list[tuple[int, str]],
    failures: list[tuple[int, str]],
) -> None:
    """Batch persist results for better database performance."""
    if not successes and not failures:
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            now = utcnow()

            # Batch update successful banners
            if successes:
                await conn.executemany(
                    """
                    UPDATE domains
                    SET banner = $2,
                        updated_at = $3
                    WHERE id = $1
                    """,
                    [(domain_id, banner, now)
                     for domain_id, banner in successes]
                )

                # Batch update success states
                await conn.executemany(
                    """
                    INSERT INTO banner_scan_state (
                        domain_id, in_progress, last_attempted_at,
                        last_failed_at, failure_reason
                    )
                    VALUES ($1, FALSE, $2, NULL, NULL)
                    ON CONFLICT (domain_id)
                    DO UPDATE SET
                        in_progress = FALSE,
                        last_attempted_at = $2,
                        last_failed_at = NULL,
                        failure_reason = NULL
                    """,
                    [(domain_id, now) for domain_id, _ in successes]
                )

            # Batch update failure states
            if failures:
                await conn.executemany(
                    """
                    INSERT INTO banner_scan_state (
                        domain_id, in_progress, last_attempted_at,
                        last_failed_at, failure_reason
                    )
                    VALUES ($1, FALSE, $2, $3, $4)
                    ON CONFLICT (domain_id)
                    DO UPDATE SET
                        in_progress = FALSE,
                        last_attempted_at = $2,
                        last_failed_at = $3,
                        failure_reason = $4
                    """,
                    [(domain_id, now, now, error[:500])
                     for domain_id, error in failures]
                )


async def process_banner_batch(
    items: list[dict[str, str | int]],
    port: int,
    timeout: float,
    pool: asyncpg.Pool,
    semaphore: asyncio.Semaphore,
) -> None:
    """Process banner scans concurrently with batch DB operations."""
    async def scan_single_domain(
        item: dict[str, str | int]
    ) -> tuple[int, str, str, str | None]:
        async with semaphore:
            domain_id = int(item["id"])
            domain = str(item["domain"])
            ip = str(item["ip"])

            banner = await asyncio.to_thread(grab_banner, ip, port, timeout)
            return domain_id, domain, ip, banner

    # Process all items in the batch concurrently
    tasks = [scan_single_domain(item) for item in items]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Separate successes and failures for batch processing
    successes: list[tuple[int, str]] = []
    failures: list[tuple[int, str]] = []

    for result in results:
        if isinstance(result, Exception):
            continue  # Skip exceptions for now

        domain_id, domain, ip, banner = result
        if banner:
            successes.append((domain_id, banner))
            print(f"INFO: grabbed banner for {domain} ({ip})")
        else:
            failures.append(
                (domain_id, f"No banner response from {ip}:{port}")
            )

    # Batch persist all results
    await persist_batch_results(pool, successes, failures)

    if successes:
        print(f"INFO: stored {len(successes)} successful banners")
    if failures:
        print(f"INFO: recorded {len(failures)} failed attempts")


async def worker(
    worker_index: int,
    pool: asyncpg.Pool,
    batch_size: int,
    port: int,
    timeout: float,
    retry_failed: bool,
    concurrent_scans: int = 10,
    limit: int | None = None,
) -> None:
    """Worker function using optimized batch processing with concurrency."""
    label = f"worker-{worker_index}"
    processed_count = 0
    start_time = time.time()

    while True:
        # Check limit before retrieving next batch
        if limit and processed_count >= limit:
            print(f"[{label}] reached limit of {limit} domains")
            break

        # Adjust batch size if we're approaching the limit
        effective_batch_size = batch_size
        if limit:
            remaining = limit - processed_count
            effective_batch_size = min(batch_size, remaining)

        # Retrieve a batch of domains to process
        batch = await retrieve_batch(
            pool, effective_batch_size, port, retry_failed
        )
        if not batch:
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            print(f"[{label}] completed {processed_count} domains "
                  f"in {elapsed:.2f}s ({rate:.1f}/sec)")
            break

        batch_count = len(batch)
        processed_count += batch_count

        print(f"[{label}] processing {batch_count} domains")
        batch_start = time.time()

        # Process the batch with optimized concurrent scanning
        semaphore = asyncio.Semaphore(concurrent_scans)
        await process_banner_batch(batch, port, timeout, pool, semaphore)

        batch_time = time.time() - batch_start
        batch_rate = batch_count / batch_time if batch_time > 0 else 0
        print(f"[{label}] batch completed: {batch_count} domains "
              f"in {batch_time:.2f}s ({batch_rate:.1f}/sec)")


async def run_async(
    postgres_dsn: str,
    worker_count: int,
    batch: int,
    port: int,
    timeout: float,
    retry_failed: bool,
    limit: int | None = None,
) -> None:
    min_size = max(1, min(worker_count, 4))
    max_size = max(worker_count * 2, min_size)

    async with asyncpg.create_pool(
        asyncpg_pool_dsn(postgres_dsn),
        min_size=min_size,
        max_size=max_size,
    ) as pool:
        await ensure_state_table(pool)

        # If limit is specified, implement simple coordination
        if limit:
            print(f"INFO: Processing up to {limit} domains")
            # For simplicity with limit, use single worker approach
            await worker(
                1, pool, batch, port, timeout, retry_failed, limit=limit
            )
        else:
            # Use multiple workers for unlimited processing
            tasks = [
                asyncio.create_task(
                    worker(i + 1, pool, batch, port, timeout, retry_failed)
                )
                for i in range(worker_count)
            ]
            await asyncio.gather(*tasks)


@click.command(help=__doc__)
@click.option(
    "--postgres-dsn",
    type=str,
    required=True,
    help="PostgreSQL DSN",
)
@click.option(
    "--worker",
    "worker_count",
    type=int,
    required=True,
    help="Number of concurrent async workers",
)
@click.option(
    "--batch",
    type=int,
    default=50,
    show_default=True,
    help="Number of domains claimed per worker fetch",
)
@click.option(
    "--port",
    type=int,
    default=DEFAULT_PORT,
    show_default=True,
    help="Port number to probe for SSH banners",
)
@click.option(
    "--timeout",
    type=float,
    default=1.0,
    show_default=True,
    help="Socket timeout (seconds) for banner grabs",
)
@click.option(
    "--retry-failed",
    is_flag=True,
    help="Re-attempt domains previously marked as failed",
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of domains to process (for testing)",
)
def main(
    postgres_dsn: str,
    worker_count: int,
    batch: int,
    port: int,
    timeout: float,
    retry_failed: bool,
    limit: int | None,
) -> None:
    limit_str = f" limit={limit}" if limit else ""
    print(
        "INFO: starting banner grabber | "
        f"workers={worker_count} batch={batch} port={port}{limit_str}"
    )
    asyncio.run(
        run_async(
            postgres_dsn,
            worker_count,
            batch,
            port,
            timeout,
            retry_failed,
            limit,
        )
    )


if __name__ == "__main__":
    main()
