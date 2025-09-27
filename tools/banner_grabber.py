#!/usr/bin/env python3

"""Collect SSH banners for domains stored in PostgreSQL."""

from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import asyncpg
import click

DEFAULT_PORT = 22
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}

    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" not in stripped:
                continue
            key, raw_value = stripped.split("=", 1)
            value = raw_value.strip().strip('"').strip("'")
            values[key.strip()] = value
    except FileNotFoundError:
        return values

    return values


def resolve_dsn() -> str:
    env_value = os.environ.get("POSTGRES_DSN")
    if env_value:
        dsn = env_value
    else:
        file_values = _parse_env_file(ENV_PATH)
        dsn = file_values.get("POSTGRES_DSN")

    if not dsn:
        raise RuntimeError(
            "POSTGRES_DSN must be set as an environment variable or in the .env file"
        )

    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://"):]
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://"):]
    return dsn


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

            ids = [int(row["id"]) for row in selections]
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
    """Attempt to read an SSH banner from ``ip:port``."""

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        banner = sock.recv(1024)
        sock.close()
        decoded = banner.decode("utf-8", errors="ignore").strip()
        if decoded:
            print(f"INFO: grabbed banner from {ip}:{port} -> {decoded}")
        else:
            print(f"WARN: empty banner from {ip}:{port}")
        return decoded
    except socket.timeout as exc:
        print(f"ERROR: timeout while grabbing banner from {ip}:{port} -> {exc}")
        return None
    except ConnectionRefusedError as exc:
        print(f"ERROR: connection refused for {ip}:{port} -> {exc}")
        return None
    except OSError as exc:
        print(f"ERROR: OS error while grabbing banner from {ip}:{port} -> {exc}")
        return None
    except Exception as exc:
        print(f"ERROR: unexpected error while grabbing banner from {ip}:{port} -> {exc}")
        raise


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
            await _finalize_state_with_conn(conn, domain_id, failed=False, failure_reason=None)


async def persist_failure(
    pool: asyncpg.Pool,
    *,
    domain_id: int,
    error: str,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _finalize_state_with_conn(conn, domain_id, failed=True, failure_reason=error)


async def worker(
    worker_index: int,
    pool: asyncpg.Pool,
    batch_size: int,
    port: int,
    timeout: float,
    retry_failed: bool,
) -> None:
    label = f"worker-{worker_index}"
    while True:
        batch = await retrieve_batch(pool, batch_size, port, retry_failed)
        if not batch:
            print(f"[{label}] no more domains to process")
            break

        print(f"[{label}] processing {len(batch)} domains")
        for item in batch:
            domain_id = int(item["id"])
            domain = str(item["domain"])
            ip = str(item["ip"])

            banner = await asyncio.to_thread(grab_banner, ip, port, timeout)
            if banner:
                await persist_success(pool, domain_id=domain_id, banner=banner)
                print(f"INFO: stored banner for {domain} ({ip})")
            else:
                await persist_failure(
                    pool,
                    domain_id=domain_id,
                    error=f"No banner response from {ip}:{port}",
                )


async def run_async(
    worker_count: int,
    batch: int,
    port: int,
    timeout: float,
    retry_failed: bool,
) -> None:
    dsn = resolve_dsn()

    min_size = max(1, min(worker_count, 4))
    max_size = max(worker_count * 2, min_size)

    async with asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size) as pool:
        await ensure_state_table(pool)
        tasks = [
            asyncio.create_task(
                worker(i + 1, pool, batch, port, timeout, retry_failed)
            )
            for i in range(worker_count)
        ]
        await asyncio.gather(*tasks)


@click.command(help=__doc__)
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
def main(
    worker_count: int,
    batch: int,
    port: int,
    timeout: float,
    retry_failed: bool,
) -> None:
    print(
        "INFO: starting banner grabber | "
        f"workers={worker_count} batch={batch} port={port}"
    )
    asyncio.run(run_async(worker_count, batch, port, timeout, retry_failed))


if __name__ == "__main__":
    main()
