"""Shared helpers for API pagination and caching."""

import math
from typing import Any, Awaitable, Callable, Dict, Iterable, Tuple

from app.cache import fetch_from_cache


PaginationResult = Dict[str, Any]


def paginate(
    *,
    page: int,
    page_size: int,
    total: int,
    results: Iterable[Any],
) -> PaginationResult:
    """Create a standard pagination payload."""

    safe_page_size = max(page_size, 1)
    total_pages = math.ceil(total / safe_page_size) if total else 0
    return {
        "page": page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_previous": page > 1 and total > 0,
        "results": list(results),
    }


async def cached_paginated_fetch(
    key: str,
    loader: Callable[[], Awaitable[Tuple[Iterable[Any], int]]],
    *,
    page: int,
    page_size: int,
    ttl: int = 300,
) -> PaginationResult:
    """Fetch ``loader`` results and wrap them in pagination metadata."""

    async def _populate() -> PaginationResult:
        data, total = await loader()
        return paginate(page=page, page_size=page_size, total=total, results=data)

    return await fetch_from_cache(key, _populate, ttl=ttl)


def cache_key(key: str) -> str:
    import re
    return re.sub(r'[\\\/\(\)\'\"\[\],;:#+~\. ]', '-', key)
