#!/usr/bin/env python3

"""Collect SSH banners for domains stored in PostgreSQL."""

from __future__ import annotations

import asyncio
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

import click

from sqlalchemy import Index, and_, bindparam, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy.orm import aliased
from sqlmodel import Field, SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from async_sqlmodel_helpers import get_engine, get_session_factory

# Ensure the repository root is on ``sys.path`` so shared modules can be imported
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from shared.models.postgres import ARecord, Domain, PortService

DEFAULT_PORT = 22


class BannerScanState(SQLModel, table=True):
    __tablename__ = "banner_scan_state"
    __table_args__ = (
        Index("ix_banner_scan_state_last_failed", "last_failed_at"),
    )

    domain_id: int = Field(primary_key=True, foreign_key="domains.id")
    in_progress: bool = Field(default=False, nullable=False)
    last_attempted_at: datetime | None = Field(default=None)
    last_failed_at: datetime | None = Field(default=None)
    failure_reason: str | None = Field(default=None)


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with SQLModel columns."""

    return datetime.now(timezone.utc).replace(tzinfo=None)


async def ensure_state_table(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: SQLModel.metadata.create_all(
                sync_conn, tables=[BannerScanState.__table__]
            )
        )


async def retrieve_batch(
    session_factory: async_sessionmaker[AsyncSession],
    batch_size: int,
    port: int,
    retry_failed: bool,
) -> list[dict[str, str | int]]:
    """Claim a batch of domains needing banner scans using SKIP LOCKED."""

    state_alias = aliased(BannerScanState)
    async with session_factory() as session:
        async with session.begin():
            failure_filter = (
                literal(True)
                if retry_failed
                else state_alias.last_failed_at.is_(None)
            )

            stmt = (
                select(Domain.id, Domain.name)
                .outerjoin(state_alias, state_alias.domain_id == Domain.id)
                .where(Domain.banner.is_(None))
                .where(
                    or_(
                        state_alias.in_progress.is_(False),
                        state_alias.in_progress.is_(None),
                    )
                )
                .where(failure_filter)
                .where(
                    select(ARecord.id)
                    .where(ARecord.domain_id == Domain.id)
                    .exists()
                )
                .order_by(Domain.updated_at.desc())
                .limit(batch_size)
                .with_for_update(of=Domain, skip_locked=True)
            )

            selections = (await session.exec(stmt)).all()

            if not selections:
                return []

            ids = list({int(row[0]) for row in selections})
            now = utcnow()

            insert_stmt = (
                insert(BannerScanState)
                .values(
                    [
                        {
                            "domain_id": domain_id,
                            "in_progress": True,
                            "last_attempted_at": now,
                        }
                        for domain_id in ids
                    ]
                )
                .on_conflict_do_update(
                    index_elements=[BannerScanState.domain_id],
                    set_={
                        "in_progress": True,
                        "last_attempted_at": now,
                    },
                )
            )
            await session.exec(insert_stmt)

            ip_stmt = (
                select(ARecord.domain_id, ARecord.ip_address)
                .where(ARecord.domain_id.in_(ids))
                .order_by(
                    ARecord.domain_id,
                    ARecord.updated_at.desc(),
                    ARecord.id.asc(),
                )
            )
            ip_rows = (await session.exec(ip_stmt)).all()

    ip_map: dict[int, str] = {}
    for row in ip_rows:
        domain_id = int(row[0])
        ip_address = row[1]
        if domain_id not in ip_map and ip_address:
            ip_map[domain_id] = ip_address

    batch: list[dict[str, str | int]] = []
    for row in selections:
        domain_id = int(row[0])
        domain_name = str(row[1])
        ip = ip_map.get(domain_id)
        if not ip:
            continue
        batch.append({"id": domain_id, "domain": domain_name, "ip": ip})

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


async def persist_batch_results(
    session_factory: async_sessionmaker[AsyncSession],
    successes: list[tuple[int, str]],
    failures: list[tuple[int, str]],
) -> None:
    """Batch persist results for better database performance."""
    if not successes and not failures:
        return

    async with session_factory() as session:
        async with session.begin():
            now = utcnow()

            if successes:
                params = [
                    {
                        "domain_id_bind": domain_id,
                        "domain_id": domain_id,
                        "banner": banner,
                        "updated_at": now,
                    }
                    for domain_id, banner in successes
                ]
                stmt = (
                    update(Domain)
                    .where(Domain.id == bindparam("domain_id_bind"))
                    .values(
                        banner=bindparam("banner"),
                        updated_at=bindparam("updated_at"),
                    )
                    .execution_options(synchronize_session=False)
                )
                await session.exec(stmt, params=params)

                state_stmt = insert(BannerScanState).values(
                    [
                        {
                            "domain_id": domain_id,
                            "in_progress": False,
                            "last_attempted_at": now,
                            "last_failed_at": None,
                            "failure_reason": None,
                        }
                        for domain_id, _ in successes
                    ]
                )
                state_stmt = state_stmt.on_conflict_do_update(
                    index_elements=[BannerScanState.domain_id],
                    set_={
                        "in_progress": False,
                        "last_attempted_at": now,
                        "last_failed_at": None,
                        "failure_reason": state_stmt.excluded.failure_reason,
                    },
                )
                await session.exec(state_stmt)

            if failures:
                values = [
                    {
                        "domain_id": domain_id,
                        "in_progress": False,
                        "last_attempted_at": now,
                        "last_failed_at": now,
                        "failure_reason": error[:500],
                    }
                    for domain_id, error in failures
                ]
                state_stmt = insert(BannerScanState).values(values)
                state_stmt = state_stmt.on_conflict_do_update(
                    index_elements=[BannerScanState.domain_id],
                    set_={
                        "in_progress": False,
                        "last_attempted_at": now,
                        "last_failed_at": now,
                        "failure_reason": state_stmt.excluded.failure_reason,
                    },
                )
                await session.exec(state_stmt)


async def process_banner_batch(
    items: list[dict[str, str | int]],
    port: int,
    timeout: float,
    session_factory: async_sessionmaker[AsyncSession],
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
    await persist_batch_results(session_factory, successes, failures)

    if successes:
        print(f"INFO: stored {len(successes)} successful banners")
    if failures:
        print(f"INFO: recorded {len(failures)} failed attempts")


async def worker(
    worker_index: int,
    session_factory: async_sessionmaker[AsyncSession],
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
            session_factory, effective_batch_size, port, retry_failed
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
        await process_banner_batch(
            batch, port, timeout, session_factory, semaphore
        )

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
    engine = get_engine(dsn=postgres_dsn)
    session_factory = get_session_factory(engine=engine)

    await ensure_state_table(engine)

    try:
        if limit:
            print(f"INFO: Processing up to {limit} domains")
            await worker(
                1,
                session_factory,
                batch,
                port,
                timeout,
                retry_failed,
                limit=limit,
            )
        else:
            tasks = [
                asyncio.create_task(
                    worker(
                        i + 1,
                        session_factory,
                        batch,
                        port,
                        timeout,
                        retry_failed,
                    )
                )
                for i in range(worker_count)
            ]
            await asyncio.gather(*tasks)
    finally:
        await engine.dispose()


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
