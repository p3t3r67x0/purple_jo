import os
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.settings import reset_settings_cache, get_settings
from app.deps import get_postgres_session


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
    # Patch email and persistence to avoid database work
    from app.routes import contact as contact_module

    async def no_email(msg):  # noqa: D401
        return None
    contact_module._send_email_async = no_email  # type: ignore

    async def noop_persist(*_args, **_kwargs):  # type: ignore
        return None

    contact_module._persist_message = noop_persist  # type: ignore

    async def noop_rate_limit(*_args, **_kwargs):  # type: ignore
        return None

    contact_module._check_rate_limit = noop_rate_limit  # type: ignore

    async def dummy_session(_request):
        yield None

    original_override = app.dependency_overrides.get(get_postgres_session)
    app.dependency_overrides[get_postgres_session] = dummy_session

    transport = ASGITransport(app=app)

    try:
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
    finally:
        if original_override is not None:
            app.dependency_overrides[get_postgres_session] = original_override
        else:
            app.dependency_overrides.pop(get_postgres_session, None)


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

    from app.routes import contact as contact_module

    counter = {"count": 0}

    async def limited_rate_limit(_session, _ip):  # type: ignore
        counter["count"] += 1
        if counter["count"] > 2:
            from fastapi import HTTPException, status

            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
            )

    contact_module._check_rate_limit = limited_rate_limit  # type: ignore

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


@pytest.mark.asyncio
async def test_captcha_required_when_secret_set(
    client: AsyncClient, monkeypatch
):
    # Set hCaptcha secret but clear contact token to force captcha path
    monkeypatch.setenv("HCAPTCHA_SECRET", "test-secret")
    monkeypatch.delenv("CONTACT_TOKEN", raising=False)
    reset_settings_cache()
    
    payload = {
        "name": "Tester",
        "email": "test@example.com",
        "subject": "Hello",
        "message": "Test without captcha token",
        # No captcha_token provided
    }
    resp = await client.post("/contact", json=payload)
    assert resp.status_code == 400
    assert "captcha_token required" in resp.json()["detail"]
