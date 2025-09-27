"""Alembic environment configuration for async SQLModel migrations."""

from __future__ import annotations

import asyncio
import os
import sys
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlmodel import SQLModel

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.settings import get_settings
from app.models import postgres  # noqa: F401 - ensure models are imported

config = context.config

if config.config_file_name:
    fileConfig(config.config_file_name)


def get_url() -> str:
    settings = get_settings()
    if settings.postgres_dsn:
        return settings.postgres_dsn

    fallback_url = config.get_main_option("sqlalchemy.url")
    if fallback_url and fallback_url != "postgresql+asyncpg://localhost/db":
        return fallback_url

    raise RuntimeError(
        "POSTGRES_DSN is required for Alembic migrations (or set a valid "
        "sqlalchemy.url in alembic.ini)."
    )


target_metadata = SQLModel.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""

    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    configuration: dict[str, Any] = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    async def _run() -> None:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
        await connectable.dispose()

    asyncio.run(_run())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
