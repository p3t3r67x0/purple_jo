from typing import Awaitable, Callable, TypeVar

import logging

from aiocache import Cache
from aiocache.serializers import PickleSerializer
from aiocache.exceptions import InvalidCacheType

from app.config import (
    CACHE_EXPIRE,
    REDIS_DB,
    REDIS_HOST,
    REDIS_NAMESPACE,
    REDIS_PASSWORD,
    REDIS_PORT,
)

T = TypeVar("T")


logger = logging.getLogger(__name__)


def _create_cache() -> Cache:
    try:
        return Cache(
            Cache.REDIS,
            endpoint=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=REDIS_DB,
            namespace=REDIS_NAMESPACE,
            serializer=PickleSerializer(),
        )
    except InvalidCacheType:
        logger.warning(
            "Redis backend unavailable, falling back to in-memory cache"
        )
        return Cache(Cache.MEMORY, serializer=PickleSerializer())


cache = _create_cache()


async def fetch_from_cache(
    key: str,
    fetch_func: Callable[[], Awaitable[T]],
    *,
    ttl: int = CACHE_EXPIRE,
    refresh: bool = False,
) -> T:
    """Retrieve ``key`` from Redis or compute it with ``fetch_func``.

    Parameters
    ----------
    key:
        Cache key to look up.
    fetch_func:
    Zero-argument coroutine that computes the value when there is a
    cache miss.
    ttl:
        Time-to-live for the cached value in seconds.
    refresh:
    When ``True`` the value is recomputed and the cache is updated
        of an existing entry.
    """

    if not refresh:
        cached = await cache.get(key)
        if cached is not None:
            return cached

    value = await fetch_func()
    await cache.set(key, value, ttl=ttl)
    return value


async def invalidate_cache(key: str) -> None:
    """Remove ``key`` from the shared Redis cache if it exists."""

    await cache.delete(key)
