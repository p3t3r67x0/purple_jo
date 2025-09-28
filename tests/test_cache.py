"""Tests for the cache helpers in :mod:`app.cache`."""

import pytest

from app import cache as cache_module


class DummyCache:
    """A minimal in-memory cache used to stub the aiocache backend."""

    def __init__(self):
        self.store = {}

    async def get(self, key):  # pragma: no cover - exercised via fetch_from_cache
        return self.store.get(key)

    async def set(self, key, value, ttl=None):  # pragma: no cover - exercised via fetch_from_cache
        self.store[key] = value

    async def delete(self, key):  # pragma: no cover - exercised via invalidate_cache
        self.store.pop(key, None)


@pytest.mark.asyncio
async def test_fetch_from_cache_returns_cached_value(monkeypatch):
    """Repeated calls should reuse the cached value without recomputing."""

    dummy_cache = DummyCache()
    monkeypatch.setattr(cache_module, "cache", dummy_cache)

    call_count = 0

    async def fetch_value():
        nonlocal call_count
        call_count += 1
        return f"value-{call_count}"

    # First call computes and stores the value.
    result_first = await cache_module.fetch_from_cache("example", fetch_value)
    assert result_first == "value-1"
    assert call_count == 1

    # Second call should reuse the cached value without executing fetch_value again.
    result_second = await cache_module.fetch_from_cache("example", fetch_value)
    assert result_second == "value-1"
    assert call_count == 1, "fetch function should not have been called a second time"


@pytest.mark.asyncio
async def test_fetch_from_cache_refresh_forces_recompute(monkeypatch):
    """Setting ``refresh=True`` should bypass the cached value."""

    dummy_cache = DummyCache()
    dummy_cache.store["example"] = "stale"
    monkeypatch.setattr(cache_module, "cache", dummy_cache)

    call_count = 0

    async def fetch_value():
        nonlocal call_count
        call_count += 1
        return f"fresh-{call_count}"

    result = await cache_module.fetch_from_cache("example", fetch_value, refresh=True)
    assert result == "fresh-1"
    assert call_count == 1
    assert dummy_cache.store["example"] == "fresh-1", "cache should have been updated with fresh value"


@pytest.mark.asyncio
async def test_invalidate_cache_removes_key(monkeypatch):
    """invalidate_cache should remove an existing entry from the backend."""

    dummy_cache = DummyCache()
    dummy_cache.store["example"] = "value"
    monkeypatch.setattr(cache_module, "cache", dummy_cache)

    await cache_module.invalidate_cache("example")
    assert "example" not in dummy_cache.store
