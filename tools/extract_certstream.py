#!/usr/bin/env python3
"""Certificate Transparency log monitor for PostgreSQL domain extraction."""

from __future__ import annotations

from importlib import import_module

try:
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

bootstrap.setup()

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import certstream
import click
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.postgres import Domain


ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_file(path: Path) -> dict:
    """Parse a .env file and return key-value pairs."""
    env_vars = {}
    if not path.exists():
        return env_vars

    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip().strip('"\'')
    return env_vars


def resolve_dsn() -> str:
    """Resolve PostgreSQL DSN from environment or .env file."""
    # First try environment variables
    if "POSTGRES_DSN" in os.environ:
        dsn = os.environ["POSTGRES_DSN"]
    else:
        # Try to load from .env file
        env_vars = _parse_env_file(ENV_PATH)
        dsn = env_vars.get("POSTGRES_DSN")

        if not dsn:
            raise ValueError(
                "POSTGRES_DSN not found in environment or .env file"
            )

    return dsn


class PostgresAsync:
    """PostgreSQL async connection manager."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.engine = create_async_engine(dsn)
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
    default=None
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose logging'
)
def main(postgres_dsn: Optional[str], verbose: bool):
    """Certificate Transparency log monitor for PostgreSQL."""
    # Configure logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format='[%(levelname)s:%(name)s] %(asctime)s - %(message)s',
        level=log_level
    )

    # Use settings or .env file if no explicit DSN provided
    if not postgres_dsn:
        try:
            postgres_dsn = resolve_dsn()
        except ValueError as exc:
            click.echo(f"[ERROR] {exc}")
            click.echo("[INFO] Please set POSTGRES_DSN in environment or "
                       ".env file")
            click.echo("[INFO] Example: POSTGRES_DSN=postgresql+asyncpg://"
                       "user:pass@localhost:5432/dbname")
            sys.exit(1)
        except Exception as exc:
            click.echo(f"[ERROR] Could not resolve PostgreSQL DSN: {exc}")
            sys.exit(1)

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
    main()
