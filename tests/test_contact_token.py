import os
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.settings import reset_settings_cache, get_settings


class DummyColl:
    def __init__(self):
        self.docs = []

    async def insert_one(self, doc):  # noqa: D401
        self.docs.append(doc)

    async def create_index(self, *a, **k):  # noqa: D401
        return None

    async def find_one_and_update(self, key, update, upsert, return_document):
        # Minimal rate-limit doc simulation
        for d in self.docs:
            if d.get("_key") == key:
                d["count"] = d.get("count", 0) + 1
                return d
        new_doc = {"_key": key, "count": 1}
        self.docs.append(new_doc)
        return new_doc


class DummyMongo:
    def __init__(self):
        self.contact_messages = DummyColl()
        self.contact_rate_limit = DummyColl()


@pytest.fixture(autouse=True)
def clear_env_and_cache(monkeypatch):
    for var in ["HCAPTCHA_SECRET", "RECAPTCHA_SECRET"]:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("CONTACT_TOKEN", "secret123")
    monkeypatch.setenv("CONTACT_RATE_LIMIT", "5")
    reset_settings_cache()
    yield
    reset_settings_cache()


@pytest_asyncio.fixture
async def client(monkeypatch):  # type: ignore[override]
    # Patch email sending to avoid network
    from app.routes import contact as contact_module

    async def no_email(msg):  # noqa: D401
        return None
    contact_module._send_email_async = no_email  # type: ignore

    # Provide in-memory mongo via app state for dependency resolution
    original_mongo = getattr(app.state, "mongo", None)
    app.state.mongo = DummyMongo()

    transport = ASGITransport(app=app)

    try:
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
    finally:
        app.state.mongo = original_mongo


@pytest.mark.asyncio
async def test_contact_token_submission_success(client: AsyncClient):
    os.environ["CONTACT_TOKEN"] = "shared-secret"
    reset_settings_cache()
    assert get_settings().contact_token == "shared-secret"
    payload = {
        "name": "Tester",
        "email": "tester@example.com",
        "subject": "Hello",
        "message": "This is a test message",
        "token": "shared-secret",
    }
    resp = await client.post("/contact", json=payload)
    assert resp.status_code == 202, resp.text
    assert resp.json()["status"] == "accepted"


@pytest.mark.asyncio
async def test_contact_token_invalid(client: AsyncClient):
    os.environ["CONTACT_TOKEN"] = "expected"
    reset_settings_cache()
    payload = {
        "name": "Tester",
        "email": "tester@example.com",
        "subject": "Hi",
        "message": "Bad token attempt",
        "token": "wrong",
    }
    resp = await client.post("/contact", json=payload)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_rate_limit_enforced(client: AsyncClient, monkeypatch):
    monkeypatch.setenv("CONTACT_RATE_LIMIT", "2")
    reset_settings_cache()
    for i in range(2):
        r = await client.post(
            "/contact",
            json={
                "name": f"Tester{i}",
                "email": "t@example.com",
                "subject": "Hi",
                "message": "Message body",
                "token": get_settings().contact_token,
            },
        )
        assert r.status_code == 202
    r = await client.post(
        "/contact",
        json={
            "name": "Overflow",
            "email": "o@example.com",
            "subject": "Hi",
            "message": "Message body",
            "token": get_settings().contact_token,
        },
    )
    assert r.status_code == 429
