import os
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.settings import reset_settings_cache
from app.deps import get_postgres_session, get_client_ip


@pytest.fixture(autouse=True)
def clear_env_and_cache():
    for var in [
        "HCAPTCHA_SECRET",
        "RECAPTCHA_SECRET",
        "CONTACT_TOKEN",
        "CONTACT_RATE_LIMIT",
        "CONTACT_RATE_WINDOW",
    ]:
        os.environ.pop(var, None)
    os.environ["CONTACT_RATE_LIMIT"] = "2"
    os.environ["CONTACT_RATE_WINDOW"] = "60"
    reset_settings_cache()
    yield
    reset_settings_cache()


@pytest.fixture
def client(monkeypatch):
    from app.routes import contact as contact_module

    async def no_email(msg):  # noqa: D401
        return None

    monkeypatch.setattr(contact_module, "_send_email_async", no_email)

    async def noop_persist(*_args, **_kwargs):
        return None

    monkeypatch.setattr(contact_module, "_persist_message", noop_persist)

    counter = {"count": 0}

    async def limited_rate_limit(_session, _ip):
        counter["count"] += 1
        if counter["count"] > 2:
            from fastapi import HTTPException, status

            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
            )

    monkeypatch.setattr(contact_module, "_check_rate_limit",
                        limited_rate_limit)

    class MockSession:
        async def rollback(self):
            pass
        
        async def commit(self):
            pass
        
        async def flush(self):
            pass
        
        def add(self, obj):
            pass

    async def dummy_session(request):
        yield MockSession()

    def dummy_client_ip():
        return "127.0.0.1"

    original_override = app.dependency_overrides.get(get_postgres_session)
    original_ip_override = app.dependency_overrides.get(get_client_ip)
    app.dependency_overrides[get_postgres_session] = dummy_session
    app.dependency_overrides[get_client_ip] = dummy_client_ip

    with TestClient(app) as test_client:
        yield test_client

    if original_override is not None:
        app.dependency_overrides[get_postgres_session] = original_override
    else:
        app.dependency_overrides.pop(get_postgres_session, None)
    
    if original_ip_override is not None:
        app.dependency_overrides[get_client_ip] = original_ip_override
    else:
        app.dependency_overrides.pop(get_client_ip, None)


def _payload(i: int):
    return {
        "name": f"User {i}",
        "email": f"user{i}@example.com",
        "subject": "Hello",
        "message": "Test message body",
    }


def test_rate_limit_exceeded(client):
    # First two should pass, third should 429
    r1 = client.post("/contact", json=_payload(1))
    assert r1.status_code == 202, r1.text
    r2 = client.post("/contact", json=_payload(2))
    assert r2.status_code == 202, r2.text
    r3 = client.post("/contact", json=_payload(3))
    assert r3.status_code == 429, r3.text
