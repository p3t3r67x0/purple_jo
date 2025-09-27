#!/usr/bin/env python3
"""
Migration script to add domain_extracted column to urls table.

This script adds the domain_extracted column needed for the extract_domains.py tool.
"""

import asyncio
import sys
from pathlib import Path

# Add the project root to the Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import click
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def add_domain_extracted_column(postgres_dsn: str):
    """Add domain_extracted column to urls table."""
    engine = create_async_engine(postgres_dsn, echo=True)
    
    try:
        # First, check if column exists and add it in a transaction
        async with engine.begin() as conn:
            # Check if column already exists
            check_stmt = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'urls' 
                AND column_name = 'domain_extracted'
            """)
            result = await conn.execute(check_stmt)
            existing = result.fetchone()
            
            if existing:
                print("✅ Column 'domain_extracted' already exists in urls table")
                column_exists = True
            else:
                # Add the column
                alter_stmt = text("""
                    ALTER TABLE urls 
                    ADD COLUMN domain_extracted TIMESTAMP
                """)
                await conn.execute(alter_stmt)
                print("✅ Added domain_extracted column to urls table")
                column_exists = False
        
        # Then, create index outside of transaction (required for CONCURRENTLY)
        if not column_exists:
            async with engine.connect() as conn:
                # Add index for performance (outside transaction for CONCURRENTLY)
                index_stmt = text("""
                    CREATE INDEX IF NOT EXISTS ix_urls_domain_extracted 
                    ON urls (domain_extracted)
                """)
                await conn.execute(index_stmt)
                print("✅ Created index on domain_extracted column")
            
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
    asyncio.run(add_domain_extracted_column(postgres_dsn))
    print("Migration completed successfully!")


if __name__ == '__main__':
    main()