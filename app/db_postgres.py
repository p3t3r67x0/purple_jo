"""PostgreSQL engine and session management using SQLModel."""

from __future__ import annotations

from functools import lru_cache
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.exc import ArgumentError, ProgrammingError
from sqlmodel import SQLModel

# Import models so SQLModel metadata is populated when init_db runs.
from app.models import postgres as postgres_models  # noqa: F401
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import get_settings


def _engine_kwargs() -> dict[str, object]:
    settings = get_settings()
    return {
        "echo": settings.postgres_echo,
        "pool_size": settings.postgres_pool_size,
        "max_overflow": settings.postgres_pool_max_overflow,
        "pool_timeout": settings.postgres_pool_timeout,
        "pool_recycle": settings.postgres_pool_recycle,
        "pool_pre_ping": True,
        "future": True,
    }


@lru_cache(maxsize=1)
def get_engine() -> AsyncEngine:
    """Return a cached async engine configured from settings."""

    settings = get_settings()
    if not settings.postgres_dsn:
        raise RuntimeError(
            "POSTGRES_DSN must be configured before using the PostgreSQL engine."
        )

    try:
        return create_async_engine(settings.postgres_dsn, **_engine_kwargs())
    except ArgumentError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Invalid POSTGRES_DSN configuration") from exc


@lru_cache(maxsize=1)
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Provide a cached async session factory bound to the engine."""

    return async_sessionmaker(
        bind=get_engine(),
        expire_on_commit=False,
        class_=AsyncSession,
    )


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async database session for request-scoped usage."""

    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


async def init_db() -> None:
    """Create database tables for the registered SQLModel metadata."""

    engine = get_engine()
    async with engine.begin() as conn:
        def _create_all(sync_conn):
            try:
                SQLModel.metadata.create_all(sync_conn, checkfirst=True)
            except ProgrammingError as exc:  # pragma: no cover - defensive
                message = str(exc).lower()
                if any(token in message for token in ("already exists", "existiert bereits", "duplicate")):
                    return
                raise

        await conn.run_sync(_create_all)


__all__ = [
    "AsyncSession",
    "SQLModel",
    "get_engine",
    "get_session",
    "get_session_factory",
    "init_db",
]
