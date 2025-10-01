#!/usr/bin/env python3
"""Certificate Transparency log monitor for PostgreSQL domain extraction."""

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
import logging
import sys
from datetime import datetime

import certstream
import click
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from shared.models.postgres import Domain
from async_sqlmodel_helpers import normalise_async_dsn, resolve_async_dsn


class PostgresAsync:
    """PostgreSQL async connection manager."""

    def __init__(self, dsn: str):
        self.dsn = normalise_async_dsn(dsn)
        self.engine = create_async_engine(self.dsn)
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def add_domain(self, domain_name: str) -> bool:
        """Add domain to PostgreSQL, returns True if added, False if exists."""
        try:
            async with self.session_factory() as session:
                # Check if domain already exists
                stmt = select(Domain).where(Domain.name == domain_name)
                result = await session.exec(stmt)
                existing = result.first()
                
                if existing:
                    return False
                
                # Add new domain
                now = datetime.now().replace(tzinfo=None)
                domain = Domain(
                    name=domain_name,
                    created_at=now,
                    updated_at=now
                )
                session.add(domain)
                await session.commit()
                return True
                
        except IntegrityError:
            # Domain already exists (unique constraint)
            return False
        except Exception as e:
            logging.error(f"Error adding domain {domain_name}: {e}")
            return False

    async def close(self):
        """Close database connections."""
        await self.engine.dispose()


def create_callback(postgres_dsn: str):
    """Create callback function with PostgreSQL connection."""
    postgres = PostgresAsync(postgres_dsn)
    
    def print_callback(message, context):
        """Process certificate transparency messages."""
        logging.debug("Message -> {}".format(message))

        if message['message_type'] == "heartbeat":
            return

        if message['message_type'] == "certificate_update":
            all_domains = message['data']['leaf_cert']['all_domains']

            if len(all_domains) == 0:
                return

            # Process all domains from the certificate
            for domain_name in all_domains:
                if not domain_name:
                    continue
                    
                # Remove wildcard prefix
                domain_name = domain_name.replace('*.', '')
                
                # Skip invalid domains
                if not domain_name or domain_name == "NULL":
                    continue
                
                # Add domain asynchronously
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    added = loop.run_until_complete(
                        postgres.add_domain(domain_name)
                    )
                    if added:
                        print(f"[NEW] {domain_name}")
                    else:
                        print(f"[EXISTS] {domain_name}")
                finally:
                    loop.close()

    return print_callback


@click.command()
@click.option(
    '--postgres-dsn',
    help='PostgreSQL DSN (e.g., postgresql+asyncpg://user:pass@host/db)',
    type=str,
    required=True,
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose logging'
)
def main(postgres_dsn: str, verbose: bool):
    """Certificate Transparency log monitor for PostgreSQL."""
    # Configure logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format='[%(levelname)s:%(name)s] %(asctime)s - %(message)s',
        level=log_level
    )

    postgres_dsn = resolve_async_dsn(postgres_dsn)

    click.echo("[INFO] Starting Certificate Transparency monitor...")
    click.echo(f"[INFO] PostgreSQL DSN: {postgres_dsn}")
    
    # Create callback with PostgreSQL connection
    callback = create_callback(postgres_dsn)
    
    try:
        certstream.listen_for_events(
            callback, url='wss://certstream.calidog.io'
        )
    except KeyboardInterrupt:
        click.echo("\n[INFO] Stopping Certificate Transparency monitor...")
    except Exception as exc:
        click.echo(f"[ERROR] Certificate stream error: {exc}")
        sys.exit(1)


if __name__ == '__main__':
    CLITool(main).run()
