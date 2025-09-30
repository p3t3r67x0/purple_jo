#!/usr/bin/env python3
"""Migration helpers for transitioning the crawler to PostgreSQL."""

from __future__ import annotations

from importlib import import_module

try:
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

project_root = bootstrap.setup()

import asyncio
import logging
import os
import sys
from datetime import datetime, UTC

import click
from sqlalchemy import func, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from async_sqlmodel_helpers import normalise_async_dsn, resolve_async_dsn

try:
    from shared.models.postgres import CrawlStatus, Domain
except ImportError as exc:  # pragma: no cover - defensive
    print("Error: Missing required dependencies. Ensure project dependencies are installed.")
    print(f"Import error: {exc}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Project root: {project_root}")
    sys.exit(1)


log = logging.getLogger(__name__)


def utcnow() -> datetime:
    """Return current UTC datetime without timezone info."""
    return datetime.now(UTC).replace(tzinfo=None)


async def get_session(postgres_dsn: str) -> AsyncSession:
    """Get a database session."""
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        normalise_async_dsn(postgres_dsn),
        echo=False,
        pool_size=10,
        max_overflow=20,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    return session_factory()


async def populate_crawl_status(postgres_dsn: str, batch_size: int = 1000) -> None:
    """Populate crawl_status table with domains from domains table."""
    log.info("Populating crawl_status table...")
    
    async with await get_session(postgres_dsn) as session:
        # Count total domains
        from sqlalchemy import func
        count_stmt = select(func.count(Domain.id))
        total_count = await session.scalar(count_stmt)
        log.info(f"Found {total_count} domains to process")
        
        # Process in batches
        offset = 0
        processed = 0
        
        while offset < total_count:
            # Get batch of domains
            stmt = select(Domain.name).offset(offset).limit(batch_size)
            result = await session.exec(stmt)
            domain_names = result.all()
            
            if not domain_names:
                break
            
            # Create crawl_status records for domains that don't have them
            for domain_name in domain_names:
                # Check if crawl_status already exists
                existing_stmt = select(CrawlStatus).where(CrawlStatus.domain_name == domain_name)
                existing = await session.exec(existing_stmt)
                if existing.first() is None:
                    # Create new crawl_status record
                    crawl_status = CrawlStatus(
                        domain_name=domain_name,
                        created_at=utcnow(),
                        updated_at=utcnow(),
                    )
                    session.add(crawl_status)
                    processed += 1
            
            # Commit batch
            await session.commit()
            offset += batch_size
            
            if processed > 0 and processed % 10000 == 0:
                log.info(f"Processed {processed} domains so far...")
        
        log.info(f"Created {processed} new crawl_status records")


async def show_statistics(postgres_dsn: str) -> None:
    """Show crawl statistics."""
    log.info("Gathering statistics...")
    
    async with await get_session(postgres_dsn) as session:
        from sqlalchemy import func
        
        # Count total domains
        total_domains = await session.scalar(func.count(Domain.id))
        
        # Count crawl status
        total_crawl_status = await session.scalar(func.count(CrawlStatus.id))
        
        # Count crawled domains
        crawled_stmt = select(func.count(CrawlStatus.id)).where(CrawlStatus.domain_crawled.is_not(None))
        crawled_count = await session.scalar(crawled_stmt)
        
        # Count failed domains
        failed_stmt = select(func.count(CrawlStatus.id)).where(CrawlStatus.crawl_failed.is_not(None))
        failed_count = await session.scalar(failed_stmt)
        
        # Count pending domains
        pending_stmt = select(func.count(CrawlStatus.id)).where(
            CrawlStatus.domain_crawled.is_(None) & 
            CrawlStatus.crawl_failed.is_(None)
        )
        pending_count = await session.scalar(pending_stmt)
        
        # Count URLs
        url_count_result = await session.execute(text("SELECT COUNT(*) FROM urls"))
        url_count = url_count_result.scalar()
        
        print("\n=== Crawl Statistics ===")
        print(f"Total domains: {total_domains}")
        print(f"Crawl status records: {total_crawl_status}")
        print(f"Crawled successfully: {crawled_count}")
        print(f"Failed crawls: {failed_count}")
        print(f"Pending crawls: {pending_count}")
        print(f"Total URLs discovered: {url_count}")
        
        if total_crawl_status > 0:
            success_rate = (crawled_count / total_crawl_status) * 100
            print(f"Success rate: {success_rate:.1f}%")


async def reset_crawl_status(postgres_dsn: str) -> None:
    """Reset all crawl status (mark all as not crawled)."""
    log.info("Resetting crawl status...")
    
    async with await get_session(postgres_dsn) as session:
        # Update all crawl_status records to remove crawl timestamps
        stmt = text("""
            UPDATE crawl_status 
            SET domain_crawled = NULL, 
                crawl_failed = NULL, 
                updated_at = :now
        """)
        await session.execute(stmt, {"now": utcnow()})
        await session.commit()
        
        log.info(f"Reset crawl status for all domains")


@click.command()
@click.option("--postgres-dsn", required=True, help="PostgreSQL DSN")
@click.option("--populate", is_flag=True, help="Populate crawl_status table from domains")
@click.option("--stats", is_flag=True, help="Show crawl statistics")
@click.option("--reset", is_flag=True, help="Reset all crawl status")
@click.option("--batch-size", type=int, default=1000, help="Batch size for processing")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), 
              default="INFO", help="Log level")
def main(
    postgres_dsn: str,
    populate: bool,
    stats: bool,
    reset: bool,
    batch_size: int,
    log_level: str,
) -> None:
    """Migration helper for crawler PostgreSQL transition."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    postgres_dsn = resolve_async_dsn(postgres_dsn)
    
    async def run_operations():
        if populate:
            await populate_crawl_status(postgres_dsn, batch_size)
        
        if reset:
            await reset_crawl_status(postgres_dsn)
        
        if stats or not (populate or reset):
            await show_statistics(postgres_dsn)
    
    asyncio.run(run_operations())


if __name__ == "__main__":
    main()