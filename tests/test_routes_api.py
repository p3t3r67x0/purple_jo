from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.main import app
from app.deps import get_mongo


@pytest.fixture
def client(monkeypatch):
    """Return a TestClient with a default mongo override."""

    app.dependency_overrides.clear()
    app.dependency_overrides[get_mongo] = lambda request: SimpleNamespace()

    from app.routes import contact as contact_module

    async def no_email(msg):  # noqa: D401
        return None

    monkeypatch.setattr(contact_module, "_send_email_async", no_email)

    original_mongo = getattr(app.state, "mongo", None)
    app.state.mongo = SimpleNamespace()

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()
    app.state.mongo = original_mongo


def test_query_route_success(client, monkeypatch):
    from app.routes import query as query_routes

    async def fake_fetch(mongo, domain, page=1, page_size=25):
        return {
            "results": [{"domain": domain, "score": 1.0}],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(query_routes, "fetch_query_domain", fake_fetch)

    response = client.get("/query/example.com")
    assert response.status_code == 200
    assert response.json()["results"][0]["domain"] == "example.com"


def test_subnet_route_success(client, monkeypatch):
    from app.routes import subnet as subnet_routes

    async def fake_fetch(mongo, prefix, page=1, page_size=25):
        return {
            "results": [{"cidr": prefix, "count": 2}],
            "total": 2,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(subnet_routes, "fetch_all_prefix", fake_fetch)

    response = client.get("/subnet/192.0.2/24")
    assert response.status_code == 200
    assert response.json()["results"][0]["cidr"] == "192.0.2/24"


def test_match_route_success(client, monkeypatch):
    from app.routes import match as match_routes

    async def fake_fetch(mongo, condition, query, page=1, page_size=25):
        return {
            "results": [{"condition": condition, "value": query}],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(match_routes, "fetch_match_condition", fake_fetch)

    response = client.get("/match/issuer:Example%20CA")
    assert response.status_code == 200
    data = response.json()
    assert data["results"][0] == {"condition": "issuer", "value": "Example CA"}


def test_dns_route_success(client, monkeypatch):
    from app.routes import dns as dns_routes

    async def fake_fetch(mongo, page=1, page_size=25):
        return {
            "results": [{"domain": "example.com", "a_record": ["203.0.113.1"]}],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(dns_routes, "fetch_latest_dns", fake_fetch)

    response = client.get("/dns")
    assert response.status_code == 200
    assert response.json()["results"][0]["domain"] == "example.com"


def test_cidr_route_success(client, monkeypatch):
    from app.routes import cidr as cidr_routes

    async def fake_fetch(mongo, page=1, page_size=25):
        return {
            "results": [{"whois": {"asn_cidr": "198.51.100.0/24"}}],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(cidr_routes, "fetch_latest_cidr", fake_fetch)

    response = client.get("/cidr")
    assert response.status_code == 200
    assert response.json()[
        "results"][0]["whois"]["asn_cidr"] == "198.51.100.0/24"


def test_ipv4_route_success(client, monkeypatch):
    from app.routes import ipv4 as ipv4_routes

    async def fake_fetch(mongo, page=1, page_size=25):
        return {
            "results": [{"a_record": "203.0.113.5", "country_code": "US"}],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(ipv4_routes, "fetch_latest_ipv4", fake_fetch)

    response = client.get("/ipv4")
    assert response.status_code == 200
    assert response.json()["results"][0]["a_record"] == "203.0.113.5"


def test_asn_route_success(client, monkeypatch):
    from app.routes import asn as asn_routes

    async def fake_fetch(mongo, page=1, page_size=25, country_code=None):
        return {
            "results": [
                {
                    "whois": {
                        "asn": "AS64500",
                        "asn_country_code": country_code or "US",
                    }
                }
            ],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(asn_routes, "fetch_latest_asn", fake_fetch)

    response = client.get("/asn", params={"country_code": "GB"})
    assert response.status_code == 200
    payload = response.json()["results"][0]["whois"]
    assert payload["asn"] == "AS64500"
    assert payload["asn_country_code"] == "GB"


def test_graph_route_success(client, monkeypatch):
    from app.routes import graph as graph_routes

    async def fake_extract(mongo, site):
        return {
            "nodes": [{"id": site, "type": "domain", "label": site}],
            "edges": [],
        }

    monkeypatch.setattr(graph_routes, "extract_graph", fake_extract)

    response = client.get("/graph/example.com")
    assert response.status_code == 200
    assert response.json()["nodes"][0]["id"] == "example.com"


def test_ip_route_success(client, monkeypatch):
    from app.routes import ip as ip_routes

    async def fake_fetch(mongo, ipv4, page=1, page_size=25):
        return {
            "results": [{"ip": ipv4, "asn": "AS64501"}],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(ip_routes, "fetch_one_ip", fake_fetch)

    response = client.get("/ip/198.51.100.10")
    assert response.status_code == 200
    assert response.json()["results"][0]["ip"] == "198.51.100.10"


def test_trends_route_success(client, monkeypatch):
    from app.routes import trends as trends_routes

    async def fake_fetch(
        mongo,
        interval="minute",
        lookback_minutes=60,
        buckets=20,
        top_paths=5,
        recent_limit=20,
        path=None,
        page=1,
        page_size=25,
    ):
        return {
            "histogram": [
                {"bucket": "2024-01-01T00:00:00", "count": 42},
            ],
            "top_paths": [],
            "recent": [],
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(trends_routes, "fetch_request_trends", fake_fetch)

    response = client.get("/trends/requests", params={"interval": "hour"})
    assert response.status_code == 200
    assert response.json()["histogram"][0]["count"] == 42


def test_contact_route_accepts_submission(client, monkeypatch):
    from app.routes import contact as contact_routes

    async def noop_async(*_args, **_kwargs):  # noqa: D401 - simple async noop
        return None

    def noop_sync(*_args, **_kwargs):
        return None

    monkeypatch.setattr(contact_routes, "_ensure_indexes", noop_async)
    monkeypatch.setattr(contact_routes, "_verify_token", noop_sync)
    monkeypatch.setattr(contact_routes, "_ip_blocked", lambda ip: False)
    monkeypatch.setattr(contact_routes, "_check_rate_limit", noop_async)
    monkeypatch.setattr(contact_routes, "_verify_captcha", noop_async)
    monkeypatch.setattr(contact_routes, "_persist_message", noop_async)
    monkeypatch.setattr(contact_routes, "_send_email_async", noop_async)

    settings = SimpleNamespace(
        contact_token=None,
        contact_ip_denylist="",
        contact_rate_limit=5,
        contact_rate_window=60,
        contact_hcaptcha_secret=None,
        contact_recaptcha_secret=None,
        smtp_host="localhost",
        smtp_port=25,
        smtp_user=None,
        smtp_password=None,
        smtp_starttls=False,
        contact_from=None,
        contact_to="hello@example.com",
    )

    monkeypatch.setattr(contact_routes, "get_settings", lambda: settings)

    response = client.post(
        "/contact",
        json={
            "name": "Jane Doe",
            "email": "jane@example.com",
            "subject": "Hi",
            "message": "Test message",
        },
    )

    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}


def test_admin_route_returns_messages(client, monkeypatch):
    from app.routes import admin as admin_routes

    class FakeCursor:
        def __init__(self, docs):
            self.docs = docs

        def sort(self, *_args, **_kwargs):
            return self

        def limit(self, _limit):
            return self

        async def to_list(self, length):
            return self.docs[:length]

    class FakeCollection:
        def __init__(self, docs):
            self.docs = docs

        def find(self, *_args, **_kwargs):
            return FakeCursor(self.docs)

    class FakeMongo:
        def __init__(self, docs):
            self.contact_messages = FakeCollection(docs)

    fake_docs = [
        {
            "name": "Jane",
            "email": "jane@example.com",
            "subject": "Hello",
            "message": "Body",
        }
    ]

    from app.routes import admin as admin_routes

    app.dependency_overrides[get_mongo] = lambda request: FakeMongo(fake_docs)
    app.dependency_overrides[admin_routes.get_current_admin] = lambda: admin_routes.AdminProfile(
        id="1",
        email="admin@example.com",
        full_name=None,
    )

    response = client.get("/admin/contact/messages")
    assert response.status_code == 200
    assert response.json()["count"] == 1


def test_graph_route_not_found_returns_404(client, monkeypatch):
    from app.routes import graph as graph_routes

    async def fake_extract(_mongo, _site):
        return {"nodes": [], "edges": []}

    monkeypatch.setattr(graph_routes, "extract_graph", fake_extract)

    response = client.get("/graph/missing.example")
    assert response.status_code == 404


def test_live_scan_websocket(client, monkeypatch):
    from app.routes import live as live_routes

    async def fake_perform_live_scan(_mongo, domain, reporter):
        await reporter({"type": "status", "detail": f"scanning {domain}"})

    monkeypatch.setattr(live_routes, "perform_live_scan",
                        fake_perform_live_scan)

    with client.websocket_connect("/live/scan/example.com") as websocket:
        message = websocket.receive_json()
        assert message == {"type": "status", "detail": "scanning example.com"}

        with pytest.raises(WebSocketDisconnect):
            websocket.receive_text()
