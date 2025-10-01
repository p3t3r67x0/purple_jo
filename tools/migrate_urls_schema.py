#!/usr/bin/env python3
"""Migration script to add domain_extracted column to urls table."""

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
import sys

import click
from sqlalchemy import Column, DateTime, Index, MetaData, Table
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import AddColumn

from async_sqlmodel_helpers import normalise_async_dsn, resolve_async_dsn


async def add_domain_extracted_column(postgres_dsn: str):
    """Add domain_extracted column to urls table."""
    engine = create_async_engine(normalise_async_dsn(postgres_dsn), echo=True)
    
    try:
        # First, check if column exists and add it in a transaction
        async with engine.begin() as conn:
            def ensure_column(sync_conn):
                local_metadata = MetaData()
                urls_table = Table("urls", local_metadata, autoload_with=sync_conn)
                if "domain_extracted" in urls_table.c:
                    return True

                new_column = Column("domain_extracted", DateTime(timezone=False))
                add_column = AddColumn(urls_table, new_column)
                sync_conn.execute(add_column)
                return False

            column_exists = await conn.run_sync(ensure_column)

        if not column_exists:
            async with engine.begin() as conn:
                def ensure_index(sync_conn):
                    local_metadata = MetaData()
                    urls_table = Table("urls", local_metadata, autoload_with=sync_conn)
                    index = Index(
                        "ix_urls_domain_extracted",
                        urls_table.c.domain_extracted,
                    )
                    index.create(sync_conn, checkfirst=True)

                await conn.run_sync(ensure_index)
                print("✅ Created index on domain_extracted column")

        if column_exists:
            print("✅ Column 'domain_extracted' already exists in urls table")
        else:
            print("✅ Added domain_extracted column to urls table")
    
    except Exception as e:
        print(f"❌ Error adding column: {e}")
        raise
    finally:
        await engine.dispose()


@click.command()
@click.option('--postgres-dsn', required=True, help='PostgreSQL DSN')
def main(postgres_dsn: str):
    """Add domain_extracted column to urls table."""
    print("Adding domain_extracted column to urls table...")
    asyncio.run(add_domain_extracted_column(resolve_async_dsn(postgres_dsn)))
    print("Migration completed successfully!")


if __name__ == '__main__':
    CLITool(main).run()
