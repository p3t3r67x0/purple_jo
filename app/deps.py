"""Dependency helpers for request-scoped PostgreSQL sessions."""

from __future__ import annotations

from typing import AsyncGenerator

from fastapi import Request
from sqlmodel.ext.asyncio.session import AsyncSession

from app.db_postgres import get_session_factory


async def get_postgres_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield a PostgreSQL session for use within request handlers."""

    session_factory = getattr(request.app.state, "postgres_session_factory", None)
    if session_factory is None:
        session_factory = get_session_factory()

    async with session_factory() as session:
        yield session
