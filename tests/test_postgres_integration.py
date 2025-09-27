
"""Integration tests exercising the Postgres-backed endpoints."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, UTC
from typing import Any, Dict

import pytest
from dotenv import load_dotenv
from fastapi.testclient import TestClient
from psycopg import sql
import psycopg
from sqlalchemy.engine import make_url
from sqlalchemy.future import select
from sqlmodel import SQLModel

from app import db_postgres
from app.db_postgres import get_engine, get_session_factory
from app.main import app
from app.models.postgres import (
    ARecord,
    Domain,
    GeoPoint,
    SSLData,
    SSLSubjectAltName,
    WhoisRecord,
)
from app.settings import reset_settings_cache

load_dotenv()


class FakeCache:
    """Minimal async cache used to observe cache interactions in tests."""

    def __init__(self) -> None:
        self.store: Dict[str, Any] = {}
        self.get_calls: int = 0
        self.set_calls: int = 0

    async def get(self, key: str) -> Any:
        self.get_calls += 1
        return self.store.get(key)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        self.store[key] = value
        self.set_calls += 1

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)

    def reset_metrics(self) -> None:
        self.get_calls = 0
        self.set_calls = 0


async def _seed_postgres() -> None:
    engine = db_postgres.get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)
        # Use checkfirst=True to avoid duplicate index creation errors
        await conn.run_sync(
            lambda sync_conn: SQLModel.metadata.create_all(
                sync_conn, checkfirst=True
            )
        )

    session_factory = get_session_factory()
    async with session_factory() as session:
        now = datetime.now(UTC).replace(tzinfo=None)

        domain = Domain(
            name="web.de",
            created_at=now,
            updated_at=now,
            country_code="DE",
            banner="example",
            header_status="200",
            header_server="nginx",
        )
        session.add(domain)
        await session.flush()

        session.add(
            ARecord(
                domain_id=domain.id,
                ip_address="203.0.113.5",
                created_at=now,
                updated_at=now,
            )
        )

        session.add(
            WhoisRecord(
                domain_id=domain.id,
                asn="AS64500",
                asn_description="Example ASN",
                asn_country_code="DE",
                asn_registry="ripencc",
                asn_cidr="203.0.113.0/24",
                updated_at=now,
            )
        )

        session.add(
            GeoPoint(
                domain_id=domain.id,
                latitude=52.52,
                longitude=13.405,
                country_code="DE",
                country="Germany",
                state="Berlin",
                city="Berlin",
                updated_at=now,
            )
        )

        ssl_record = SSLData(
            domain_id=domain.id,
            issuer_common_name="Integration Issuer",
            subject_common_name="web.de",
            not_before=now,
            not_after=now,
            updated_at=now,
        )
        session.add(ssl_record)
        await session.flush()

        session.add(SSLSubjectAltName(ssl_id=ssl_record.id, value="web.de"))

        await session.commit()


async def _update_a_record(new_ip: str) -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        result = await session.exec(select(ARecord))
        record = result.scalars().first()
        assert record is not None
        record.ip_address = new_ip
        record.updated_at = datetime.now(UTC).replace(tzinfo=None)
        await session.commit()


@pytest.fixture
def postgres_client(monkeypatch):
    base_dsn = (
        os.environ.get("POSTGRES_TEST_ADMIN_DSN")
        or os.environ.get("POSTGRES_DSN")
    )
    if not base_dsn:
        pytest.skip(
            "POSTGRES_DSN (or POSTGRES_TEST_ADMIN_DSN) must be set "
            "in .env for integration tests"
        )

    base_sync_dsn = base_dsn.replace(
        "postgresql+asyncpg://", "postgresql://", 1
    )
    base_url = make_url(base_sync_dsn)
    admin_url = base_url.set(database="postgres")

    dbname = f"purple_jo_test_{uuid.uuid4().hex}"
    admin_conn = psycopg.connect(str(admin_url), autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
        )
    admin_conn.close()

    async_dsn = str(
        base_url.set(drivername="postgresql+asyncpg", database=dbname)
    )

    monkeypatch.setenv("USE_POSTGRES", "true")
    monkeypatch.setenv("POSTGRES_DSN", async_dsn)
    monkeypatch.setenv("POSTGRES_POOL_SIZE", "1")
    monkeypatch.setenv("POSTGRES_POOL_MAX_OVERFLOW", "0")

    reset_settings_cache()
    get_engine.cache_clear()
    get_session_factory.cache_clear()

    fake_cache = FakeCache()
    from app import cache as cache_module

    original_cache = cache_module.cache
    cache_module.cache = fake_cache

    asyncio.run(_seed_postgres())

    async def _noop_init_db() -> None:
        return None

    monkeypatch.setattr(db_postgres, "init_db", _noop_init_db)
    from app import main as main_module

    monkeypatch.setattr(main_module, "init_db", _noop_init_db)
    
    # Override the postgres session dependency to use our test database
    from app.deps import get_postgres_session
    
    async def get_test_postgres_session():
        session_factory = get_session_factory()
        async with session_factory() as session:
            yield session
    
    app.dependency_overrides[get_postgres_session] = get_test_postgres_session

    with TestClient(app) as client:
        yield {
            "client": client,
            "cache": fake_cache,
            "dsn": async_dsn,
        }

    app.dependency_overrides.clear()
    cache_module.cache = original_cache

    try:
        engine = db_postgres.get_engine()
        if engine:
            # Avoid event loop issues during teardown
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Schedule disposal for later if loop is running
                    loop.create_task(engine.dispose())
                else:
                    asyncio.run(engine.dispose())
            except Exception:
                # Ignore cleanup errors during test teardown
                pass
    except RuntimeError:
        # Engine not available, nothing to clean up
        pass

    reset_settings_cache()
    get_engine.cache_clear()
    get_session_factory.cache_clear()

    admin_conn = psycopg.connect(str(admin_url), autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()"
            ),
            (dbname,),
        )
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(
                sql.Identifier(dbname)
            )
        )
    admin_conn.close()


def test_dns_endpoint_reads_postgres_records(postgres_client):
    client = postgres_client["client"]

    response = client.get("/dns")
    assert response.status_code == 200

    payload = response.json()
    assert payload["total"] == 1
    result = payload["results"][0]
    assert result["domain"] == "web.de"
    assert result["a_record"] == ["203.0.113.5"]
    assert result["whois"]["asn"] == "AS64500"


def test_match_site_uses_postgres_backend(postgres_client):
    client = postgres_client["client"]

    response = client.get("/match/site%3Aweb.de")
    assert response.status_code == 200

    payload = response.json()
    assert payload["total"] == 1
    result = payload["results"][0]
    assert result["domain"] == "web.de"
    assert result["a_record"] == ["203.0.113.5"]


def test_dns_endpoint_caches_paginated_response(postgres_client):
    client = postgres_client["client"]
    cache: FakeCache = postgres_client["cache"]

    cache.reset_metrics()

    first_response = client.get("/dns")
    assert first_response.status_code == 200
    first_ip = first_response.json()["results"][0]["a_record"][0]
    assert cache.set_calls == 1

    asyncio.run(_update_a_record("198.51.100.7"))

    second_response = client.get("/dns")
    assert second_response.status_code == 200
    second_ip = second_response.json()["results"][0]["a_record"][0]

    assert second_ip == first_ip
    assert cache.set_calls == 1
    assert cache.get_calls >= 2
