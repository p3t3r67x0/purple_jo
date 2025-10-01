"""Async SQLModel helpers for tools scripts without depending on the app package."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


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


def normalise_async_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return dsn
    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]
    if "://" not in dsn:
        return f"postgresql+asyncpg://{dsn}"
    return dsn


def asyncpg_pool_dsn(dsn: str) -> str:
    """Return a DSN compatible with asyncpg based on the provided input."""

    normalised = normalise_async_dsn(dsn)
    try:
        url = make_url(normalised)
    except ArgumentError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Invalid POSTGRES_DSN configuration") from exc

    drivername = url.drivername.split("+", 1)[0]
    updated_url = url.set(drivername=drivername)
    return updated_url.render_as_string(hide_password=False)


def resolve_async_dsn(explicit: Optional[str] = None) -> str:
    if explicit:
        dsn = explicit
    elif "POSTGRES_DSN" in os.environ:
        dsn = os.environ["POSTGRES_DSN"]
    else:
        env_vars = _parse_env_file(ENV_PATH)
        dsn = env_vars.get("POSTGRES_DSN")

    if not dsn:
        raise RuntimeError("POSTGRES_DSN must be provided via flag, env var, or .env file")

    return normalise_async_dsn(dsn)


@lru_cache(maxsize=1)
def get_engine(*, dsn: Optional[str] = None) -> AsyncEngine:
    try:
        resolved = resolve_async_dsn(dsn)
        return create_async_engine(
            resolved,
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
            future=True,
        )
    except ArgumentError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Invalid POSTGRES_DSN configuration") from exc


def get_session_factory(
    *, dsn: Optional[str] = None, engine: Optional[AsyncEngine] = None
) -> async_sessionmaker[AsyncSession]:
    if engine is None:
        engine = get_engine(dsn=dsn)
    return async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)


async def init_db(*, engine: Optional[AsyncEngine] = None) -> None:
    if engine is None:
        engine = get_engine()

    async with engine.begin() as conn:
        async def _create_all(sync_conn):  # type: ignore[override]
            try:
                SQLModel.metadata.create_all(sync_conn, checkfirst=True)
            except ProgrammingError as exc:  # pragma: no cover - defensive
                message = str(exc).lower()
                if any(token in message for token in ("already exists", "duplicate")):
                    return
                raise

        await conn.run_sync(_create_all)


async def dispose_engine() -> None:
    try:
        engine = get_engine()
        await engine.dispose()
    finally:  # pragma: no cover - defensive cleanup
        get_engine.cache_clear()


__all__ = [
    "AsyncSession",
    "asyncpg_pool_dsn",
    "dispose_engine",
    "get_engine",
    "get_session_factory",
    "init_db",
    "normalise_async_dsn",
    "resolve_async_dsn",
]
