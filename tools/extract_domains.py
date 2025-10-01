#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from importlib import import_module

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

try:
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

bootstrap.setup()

import asyncio
import math
import multiprocessing
import re
import sys
import time
from datetime import datetime, UTC

import click
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from shared.models.postgres import Domain, Url
from async_sqlmodel_helpers import normalise_async_dsn, resolve_async_dsn


class DomainExtractor:
    """Extract domains from URLs and persist them to PostgreSQL."""

    @staticmethod
    def utcnow() -> datetime:
        """Return current UTC datetime without timezone info."""

        return datetime.now(UTC).replace(tzinfo=None)

    @classmethod
    async def get_postgres_session(cls, postgres_dsn: str) -> AsyncSession:
        """Create a PostgreSQL session."""

        engine = create_async_engine(
            normalise_async_dsn(postgres_dsn),
            echo=False,
            pool_size=5,
            max_overflow=10,
        )
        session_factory = async_sessionmaker(
            bind=engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )
        return session_factory()

    @staticmethod
    def match_ipv4(ipv4: str):
        return re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', ipv4)

    @staticmethod
    def find_domain(domain: str):
        return re.search(
            r'([\w\-.]{1,63}|[\w\-.]{1,63}[^\x00-\x7F\w-]{1,63})\.([\w\-.]{2,})|'
            r'(([\w\d-]{1,63}|[\d\w-]*[^\x00-\x7F\w-]{1,63}))\.?'
            r'([\w\d]{1,63}|[\d\w\-.]*[^\x00-\x7F\-.]{1,63})\.'
            r'([a-z\.]{2,}|[\w]*[^\x00-\x7F\.]{2,})',
            domain,
        )

    @classmethod
    async def retrieve_urls(
        cls,
        session: AsyncSession,
        skip: int,
        limit: int,
    ):
        """Retrieve URLs that haven't had domains extracted yet."""

        stmt = (
            select(Url)
            .where(Url.domain_extracted.is_(None))
            .order_by(Url.id.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await session.exec(stmt)
        return result.all()

    @classmethod
    async def update_url(cls, session: AsyncSession, url_id: int) -> None:
        """Mark URL as having domain extracted."""

        stmt = select(Url).where(Url.id == url_id)
        result = await session.exec(stmt)
        url = result.first()
        if url:
            url.domain_extracted = cls.utcnow()
            session.add(url)
            await session.commit()

    @classmethod
    async def add_domains(cls, session: AsyncSession, url_id: int, domain: str) -> None:
        """Add domain and mark URL as processed."""

        await cls.update_url(session, url_id)
        try:
            # Check if domain already exists
            stmt = select(Domain).where(Domain.name == domain.lower())
            result = await session.exec(stmt)
            existing_domain = result.first()

            if not existing_domain:
                # Create new domain
                new_domain = Domain(
                    name=domain.lower(),
                    created_at=cls.utcnow(),
                    updated_at=cls.utcnow(),
                )
                session.add(new_domain)
                await session.commit()
                print(f"INFO: added domain {domain} with id {new_domain.id}")
            else:
                print(f"INFO: domain {domain} already exists")
        except IntegrityError as exc:
            await session.rollback()
            print(f"Duplicate domain {domain}: {exc}")
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            print(f"Error adding domain {domain}: {exc}")
            time.sleep(1)

    @classmethod
    async def worker_async(cls, postgres_dsn: str, skip: int, limit: int) -> str:
        """Async worker to process URLs and extract domains."""

        session = await cls.get_postgres_session(postgres_dsn)

        try:
            urls = await cls.retrieve_urls(session, skip, limit)
            for url in urls:
                try:
                    domain = cls.find_domain(url.url)
                    if domain is not None and not cls.match_ipv4(domain.group(0)):
                        print(f"INFO: processing {url.url}")
                        await cls.add_domains(session, url.id, domain.group(0))
                except ValueError:
                    continue
                except Exception as exc:  # noqa: BLE001
                    print(f"ERROR processing {url.url}: {exc}")
                    continue
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR in batch {skip}:{skip+limit}: {exc}")
        finally:
            await session.close()
        return f"Worker {skip}:{skip+limit} done"

    @classmethod
    def worker(cls, task):
        """Synchronous wrapper for the async worker."""

        postgres_dsn, skip, limit = task
        return asyncio.run(cls.worker_async(postgres_dsn, skip, limit))

    @classmethod
    async def count_unprocessed_urls(cls, postgres_dsn: str) -> int:
        """Count URLs that haven't had domains extracted."""

        session = await cls.get_postgres_session(postgres_dsn)
        try:
            stmt = select(Url).where(Url.domain_extracted.is_(None))
            result = await session.exec(stmt)
            urls = result.all()
            return len(urls)
        finally:
            await session.close()


@click.command()
@click.option('--workers', required=True, type=int, help='number of workers')
@click.option('--postgres-dsn', required=True, type=str, help='PostgreSQL DSN')
def main(workers, postgres_dsn):
    """Extract domains from URLs using PostgreSQL backend."""
    postgres_dsn = resolve_async_dsn(postgres_dsn)

    total_docs = asyncio.run(DomainExtractor.count_unprocessed_urls(postgres_dsn))
    print(f"Found {total_docs} URLs to process")

    if total_docs == 0:
        print("No URLs to process")
        return

    chunk_size = math.ceil(total_docs / workers)
    tasks = [(postgres_dsn, i * chunk_size, chunk_size)
             for i in range(workers)]

    with multiprocessing.Pool(processes=workers) as pool:
        for result in pool.imap_unordered(DomainExtractor.worker, tasks):
            print(result)


if __name__ == '__main__':
    CLITool(main).run()
