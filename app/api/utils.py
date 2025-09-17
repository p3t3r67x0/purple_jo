# app/api/utils.py
import math
from typing import Any, Awaitable, Callable, Dict, Iterable, Tuple

from bson import ObjectId

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


def fix_mongo_ids(doc):
    """
    Recursively convert ObjectId fields in MongoDB documents to strings
    so they can be serialized as JSON.
    """
    if isinstance(doc, list):
        return [fix_mongo_ids(d) for d in doc]
    if isinstance(doc, dict):
        return {k: fix_mongo_ids(v) for k, v in doc.items()}
    if isinstance(doc, ObjectId):
        return str(doc)
    return doc


def cache_key(key: str) -> str:
    import re
    return re.sub(r'[\\\/\(\)\'\"\[\],;:#+~\. ]', '-', key)


def extra_fields(context: str) -> dict:
    data = {
        'created_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$created'}},
        'updated_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$updated'}},
        'domain_crawled_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$domain_crawled'}},
        'header_scan_failed_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$header_scan_failed'}},
        'ssl.not_after_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$ssl.not_after'}},
        'ssl.not_before_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$ssl.not_before'}}
    }
    if context == 'text':
        data['score'] = {'$meta': "textScore"}
    return data
