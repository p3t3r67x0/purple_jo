import aiocache

cache = aiocache.Cache(aiocache.SimpleMemoryCache)

async def fetch_from_cache(key: str, fetch_func, *args, ttl: int = 60, **kwargs):
    """Check cache first; if miss, fetch and store."""
    val = await cache.get(key)
    if val is not None:
        return val
    val = await fetch_func(*args, **kwargs)
    await cache.set(key, val, ttl=ttl)
    return val
