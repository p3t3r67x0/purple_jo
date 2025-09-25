import os
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.settings import reset_settings_cache
from tests.utils import build_test_mongo


@pytest.fixture(autouse=True)
def base_env():
    for var in [
        "HCAPTCHA_SECRET",
        "RECAPTCHA_SECRET",
        "CONTACT_TOKEN",
    ]:
        os.environ.pop(var, None)
    reset_settings_cache()
    yield
    reset_settings_cache()


@pytest.fixture
def client(monkeypatch):
    from app.routes import contact as contact_module

    async def no_email(msg):  # noqa: D401
        return None

    monkeypatch.setattr(contact_module, "_send_email_async", no_email)
    original_mongo = getattr(app.state, "mongo", None)
    app.state.mongo = build_test_mongo()
    with TestClient(app) as test_client:
        yield test_client
    app.state.mongo = original_mongo


def _payload(**overrides):
    base = {
        "name": "Alice",
        "email": "alice@example.com",
        "subject": "Hi",
        "message": "Hello world message",
    }
    base.update(overrides)
    return base


def test_missing_captcha_when_required(client, monkeypatch):
    os.environ["HCAPTCHA_SECRET"] = "secret123"
    reset_settings_cache()
    r = client.post("/contact", json=_payload())
    assert r.status_code == 400
    data = r.json()
    assert data["detail"] == "captcha_token required"


def test_invalid_captcha_response(client, monkeypatch):
    # Provide hcaptcha secret so branch uses hcaptcha
    os.environ["HCAPTCHA_SECRET"] = "secret123"
    reset_settings_cache()

    def fake_post(url, data, *args, **kwargs):  # noqa: D401
        class FakeResp:
            def json(self_inner):
                return {"success": False}
        return FakeResp()

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, data):
            return fake_post(url, data)

    # Patch httpx.AsyncClient
    import app.routes.contact as contact_mod
    contact_mod.httpx.AsyncClient = FakeClient  # type: ignore

    r = client.post(
        "/contact",
        json=_payload(captcha_token="bad"),
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid captcha"


def test_token_fallback_success(client):
    os.environ["CONTACT_TOKEN"] = "t123"
    reset_settings_cache()
    r = client.post(
        "/contact",
        json=_payload(token="t123"),
    )
    assert r.status_code == 202, r.text
