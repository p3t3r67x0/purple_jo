import os
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.settings import reset_settings_cache
from tests.utils import build_test_mongo


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
    # Patch mongo dependency
    original_mongo = getattr(app.state, "mongo", None)
    test_mongo = build_test_mongo()
    app.state.mongo = test_mongo  # simple approach; deps return this
    with TestClient(app) as test_client:
        yield test_client
    app.state.mongo = original_mongo


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
