from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

from app.api.queries import _domain_base_query
from app.models.postgres import CNAMERecord, TXTRecord
from app.services.live_scan import _persist_live_result_postgres


class FakeAsyncSession:
    def __init__(self, sync_session: Session):
        self._session = sync_session

    async def exec(self, statement):
        return self._session.exec(statement)

    async def flush(self) -> None:
        self._session.flush()

    async def commit(self) -> None:
        self._session.commit()

    async def rollback(self) -> None:
        self._session.rollback()

    def add(self, obj) -> None:
        self._session.add(obj)


@pytest.mark.asyncio
async def test_persist_live_result_stores_cname_and_txt_records():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)

    timestamp = datetime.now(timezone.utc)
    live_result = {
        "domain": "example.org",
        "header": {"status": 200, "server": "nginx"},
        "banner": "test",
        "geo": {
            "country": "United States",
            "country_code": "US",
            "state": "CA",
            "city": "San Francisco",
        },
        "a_record": ["198.51.100.10"],
        "aaaa_record": [],
        "ns_record": [],
        "soa_record": [],
        "mx_record": [],
        "cname_record": ["alias.example.org"],
        "txt_record": ["v=spf1 include:example.org"],
        "ports": [],
        "whois": {},
        "ssl": {},
    }

    with Session(engine) as sync_session:
        session = FakeAsyncSession(sync_session)
        await _persist_live_result_postgres(session, live_result, timestamp)

    with Session(engine) as sync_session:
        session = FakeAsyncSession(sync_session)
        cname_rows = (
            await session.exec(select(CNAMERecord.target))
        ).all()
        txt_rows = (
            await session.exec(select(TXTRecord.content))
        ).all()
        assert cname_rows == ["alias.example.org"]
        assert txt_rows == ["v=spf1 include:example.org"]

        domain_result = await session.exec(_domain_base_query())
        domain = domain_result.scalars().one()
        assert [record.target for record in domain.cname_records] == [
            "alias.example.org"
        ]
        assert [record.content for record in domain.txt_records] == [
            "v=spf1 include:example.org"
        ]
