from datetime import datetime

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from app.main import app
from app.deps import get_postgres_session, get_client_ip


@pytest.fixture
def client(monkeypatch):
    """Return a TestClient with a dummy database session override."""

    app.dependency_overrides.clear()

    class MockSession:
        async def rollback(self):
            pass
        
        async def commit(self):
            pass
        
        async def flush(self):
            pass
        
        def add(self, obj):
            pass

    async def dummy_session(request: Request):
        yield MockSession()
    
    def dummy_client_ip():
        return "127.0.0.1"

    app.dependency_overrides[get_postgres_session] = dummy_session
    app.dependency_overrides[get_client_ip] = dummy_client_ip

    from app.routes import contact as contact_module

    async def no_email(msg):  # noqa: D401
        return None

    monkeypatch.setattr(contact_module, "_send_email_async", no_email)

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


def test_query_route_success(client, monkeypatch):
    from app.routes import query as query_routes

    async def fake_fetch(domain, *, session, page=1, page_size=25):
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

    async def fake_fetch(prefix, *, session, page=1, page_size=25):
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

    async def fake_fetch(condition, query, *, session, page=1, page_size=25):
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

    async def fake_fetch(*, session, page=1, page_size=25):
        return {
            "results": [{"domain": "example.com",
                         "a_record": ["203.0.113.1"]}],
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

    async def fake_fetch(*, session, page=1, page_size=25):
        return {
            "results": [{"whois": {"asn_cidr": "198.51.100.0/24"}}],
            "total": 1,
            "page": page,
            "page_size": page_size,
        }

    monkeypatch.setattr(cidr_routes, "fetch_latest_cidr", fake_fetch)

    response = client.get("/cidr")
    assert response.status_code == 200
    result = response.json()["results"][0]["whois"]["asn_cidr"]
    assert result == "198.51.100.0/24"


def test_ipv4_route_success(client, monkeypatch):
    from app.routes import ipv4 as ipv4_routes

    async def fake_fetch(*, session, page=1, page_size=25):
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

    async def fake_fetch(*, session, page=1, page_size=25, country_code=None):
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

    async def fake_extract(site, *, session):
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

    async def fake_fetch(session, ipv4, *, page=1, page_size=25):
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
        *,
        session,
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
            "interval": interval,
            "lookback_minutes": lookback_minutes,
            "since": datetime.now().isoformat(),
            "path_filter": path,
            "bucket_count": 1,
            "total_requests": 1,
            "timeline": {"results": [{"count": 42}], "total": 1},
            "top_paths": [],
            "recent_requests": [],
        }

    monkeypatch.setattr(trends_routes, "fetch_request_trends", fake_fetch)

    response = client.get("/trends/requests", params={"interval": "hour"})
    assert response.status_code == 200
    assert response.json()["timeline"]["results"][0]["count"] == 42


def test_contact_route_accepts_submission(client, monkeypatch):
    from app.routes import contact as contact_routes

    async def noop_async(*_args, **_kwargs):  # noqa: D401 - simple async noop
        return None

    monkeypatch.setattr(contact_routes, "_check_rate_limit", noop_async)
    monkeypatch.setattr(contact_routes, "_persist_message", noop_async)

    response = client.post(
        "/contact",
        json={
            "name": "Alice",
            "email": "alice@example.com",
            "subject": "Hello",
            "message": "Testing",
        },
    )
    assert response.status_code == 202
    assert response.json()["status"] == "accepted"


def test_live_websocket_handles_disconnect(monkeypatch):
    from app.routes import live as live_routes

    async def fake_scan(domain, reporter, postgres_session):
        await reporter({"type": "progress", "step": "dns",
                        "status": "started"})

    monkeypatch.setattr(live_routes, "perform_live_scan", fake_scan)

    with TestClient(app) as websocket_client:
        websocket_url = "/live/scan/example.com"
        with websocket_client.websocket_connect(websocket_url) as websocket:
            # Receive initial message to ensure connection is established
            message = websocket.receive_json()
            assert message["type"] == "progress"
            # Close the connection - this should be handled gracefully
            websocket.close()
            # Test passes if no exception is raised during close


def test_live_websocket_streams_events(monkeypatch):
    from app.routes import live as live_routes

    async def fake_scan(domain, reporter, postgres_session):
        await reporter({"type": "progress", "step": "dns",
                        "status": "started"})
        await reporter({"type": "result", "data": {"domain": domain}})

    monkeypatch.setattr(live_routes, "perform_live_scan", fake_scan)

    with TestClient(app) as websocket_client:
        websocket_url = "/live/scan/example.com"
        with websocket_client.websocket_connect(websocket_url) as websocket:
            message1 = websocket.receive_json()
            message2 = websocket.receive_json()
            assert message1["type"] == "progress"
            assert message2["type"] == "result"


def test_route_not_found(client):
    response = client.get("/nonexistent")
    assert response.status_code == 404
