#!/usr/bin/env python3

import asyncio
import os
from pathlib import Path
import asyncpg


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
    
    # Convert SQLAlchemy DSN to asyncpg format
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://"):]
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://"):]
    return dsn


async def add_qrcode_column():
    """Add the qrcode column to the domains table."""
    postgres_dsn = resolve_dsn()
    conn = await asyncpg.connect(postgres_dsn)
    
    try:
        # Check if qrcode column exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'domains' AND column_name = 'qrcode'
            )
        """)
        
        if not exists:
            # Add the qrcode column
            await conn.execute("ALTER TABLE domains ADD COLUMN qrcode TEXT")
            print("[SUCCESS] Added qrcode column to domains table")
        else:
            print("[INFO] QR code column already exists")
            
    except Exception as e:
        print(f"[ERROR] Failed to add qrcode column: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(add_qrcode_column())