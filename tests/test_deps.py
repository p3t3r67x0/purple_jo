"""Unit tests for helper functions in :mod:`app.deps`."""

from types import SimpleNamespace

from app import deps


def test_get_client_ip_returns_host_when_present():
    request = SimpleNamespace(client=SimpleNamespace(host="203.0.113.9"))
    assert deps.get_client_ip(request) == "203.0.113.9"


def test_get_client_ip_returns_unknown_when_client_missing():
    request = SimpleNamespace(client=None)
    assert deps.get_client_ip(request) == "unknown"
