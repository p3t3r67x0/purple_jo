"""Shared SQLModel helpers for synchronous tooling scripts."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, Optional

from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
_ENGINE_CACHE: Dict[str, Engine] = {}


def _parse_env_file(path: Path) -> Dict[str, str]:
    env_vars: Dict[str, str] = {}
    if not path.exists():
        return env_vars

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_vars[key.strip()] = value.strip().strip('\"\'')
    return env_vars


def normalise_sync_dsn(dsn: str) -> str:
    """Normalise DSNs so SQLAlchemy uses the psycopg driver."""

    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql+psycopg://" + dsn[len("postgresql+asyncpg://") :]
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn[len("postgresql://") :]
    if dsn.startswith("postgresql+psycopg://"):
        return dsn
    if "://" not in dsn:
        return f"postgresql+psycopg://{dsn}"
    return dsn


def resolve_sync_dsn(explicit: Optional[str] = None) -> str:
    """Resolve a PostgreSQL DSN for synchronous SQLModel scripts."""

    if explicit:
        dsn = explicit
    elif "POSTGRES_DSN" in os.environ:
        dsn = os.environ["POSTGRES_DSN"]
    else:
        env_vars = _parse_env_file(ENV_PATH)
        dsn = env_vars.get("POSTGRES_DSN")

    if not dsn:
        raise ValueError("POSTGRES_DSN not provided via flag, env var, or .env file")

    return normalise_sync_dsn(dsn)


def get_engine(dsn: str) -> Engine:
    """Return (and cache) a synchronous SQLModel engine for the DSN."""

    normalised = normalise_sync_dsn(dsn)
    engine = _ENGINE_CACHE.get(normalised)
    if engine is None:
        engine = create_engine(
            normalised,
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
            future=True,
        )
        _ENGINE_CACHE[normalised] = engine
    return engine


@contextmanager
def session_scope(*, dsn: Optional[str] = None, engine: Optional[Engine] = None) -> Iterator[Session]:
    """Context manager yielding a SQLModel session tied to the given DSN/engine."""

    if engine is None:
        if dsn is None:
            raise ValueError("Either dsn or engine must be provided")
        engine = get_engine(dsn)

    with Session(engine) as session:
        yield session
