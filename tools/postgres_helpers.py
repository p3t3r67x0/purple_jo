"""Shared helpers for resolving PostgreSQL DSNs for the tools scripts."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_file(path: Path) -> Dict[str, str]:
    """Return key/value pairs from a simple ``.env`` style file."""

    if not path.exists():
        return {}

    env: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip('"\'')
    return env


def load_postgres_dsn(explicit: Optional[str] = None) -> str:
    """Resolve the PostgreSQL DSN from CLI, environment variables, or ``.env``."""

    if explicit and explicit.strip():
        candidate = explicit.strip()
    elif "POSTGRES_DSN" in os.environ:
        candidate = os.environ["POSTGRES_DSN"].strip()
    else:
        candidate = _parse_env_file(_ENV_PATH).get("POSTGRES_DSN", "").strip()

    if not candidate:
        raise RuntimeError(
            "POSTGRES_DSN must be provided via --postgres-dsn, POSTGRES_DSN env var, or .env file"
        )

    return candidate


def normalise_asyncpg_dsn(dsn: str) -> str:
    """Convert SQLAlchemy-style DSNs so they work with asyncpg create_pool/connect."""

    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://") :]
    if dsn.startswith("postgres://"):
        return "postgresql://" + dsn[len("postgres://") :]
    return dsn


__all__ = ["load_postgres_dsn", "normalise_asyncpg_dsn"]
