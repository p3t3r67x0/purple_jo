"""Helpers for aggregating request statistics.

This module exposes helpers that operate on the ``stats_data`` collection,
which is populated by :func:`app.middleware.log_stats`.  The helpers focus on
producing concise, trend friendly datasets that can be consumed by the API
layer.
"""

from datetime import datetime, timedelta
from typing import Dict, Literal, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.utils import paginate
from app.cache import fetch_from_cache

# Mapping of supported aggregation intervals to the MongoDB format string used
# in ``$dateToString`` and the Python ``timedelta`` that represents the bucket
# width.
_DATE_FORMATS: Dict[
    Literal["minute", "hour", "day"], Tuple[str, timedelta]
] = {
    "minute": ("%Y-%m-%dT%H:%M:00", timedelta(minutes=1)),
    "hour": ("%Y-%m-%dT%H:00:00", timedelta(hours=1)),
    "day": ("%Y-%m-%dT00:00:00", timedelta(days=1)),
}

_PARSE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _pagination_bounds(page: int, page_size: int) -> Tuple[int, int]:
    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    return (safe_page - 1) * safe_page_size, safe_page_size


async def fetch_request_trends(
    mongo: AsyncIOMotorDatabase,
    *,
    interval: Literal["minute", "hour", "day"],
    lookback_minutes: int,
    buckets: int,
    top_paths: int,
    recent_limit: int,
    path: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
) -> dict:
    """Aggregate request statistics for the trend endpoint.

    Parameters
    ----------
    mongo:
        The MongoDB database instance.
    interval:
        Aggregation granularity (minute, hour or day).
    lookback_minutes:
        Time window, in minutes, that the aggregation should cover.
    buckets:
        Maximum number of time buckets to return.
    top_paths:
        Number of most requested paths to include in the summary.
    recent_limit:
        Number of raw request documents to return for quick inspection.
    path:
        Optional path filter to scope the results to a specific endpoint.
    """

    since = datetime.now() - timedelta(minutes=lookback_minutes)
    match_stage: Dict[str, object] = {"created": {"$gte": since}}
    if path:
        match_stage["path"] = path

    format_str, bucket_delta = _DATE_FORMATS[interval]

    skip, limit = _pagination_bounds(page, page_size)

    async def loader() -> dict:
        timeline_pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": format_str,
                            "date": "$created",
                        }
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": -1}},
            {"$limit": buckets},
        ]
        if skip:
            timeline_pipeline.append({"$skip": skip})
        timeline_pipeline.extend([{"$limit": limit}, {"$sort": {"_id": 1}}])

        timeline_cursor = mongo.stats_data.aggregate(timeline_pipeline)
        timeline_docs = [doc async for doc in timeline_cursor]

        timeline = []
        for doc in timeline_docs:
            bucket_start = datetime.strptime(doc["_id"], _PARSE_FORMAT)
            timeline.append(
                {
                    "window_start": bucket_start.isoformat(),
                    "window_end": (bucket_start + bucket_delta).isoformat(),
                    "count": doc["count"],
                }
            )

        count_cursor = mongo.stats_data.aggregate(
            [
                {"$match": match_stage},
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {
                                "format": format_str,
                                "date": "$created",
                            }
                        }
                    }
                },
                {"$count": "count"},
            ]
        )
        count_docs = await count_cursor.to_list(length=1)
        total_buckets = count_docs[0]["count"] if count_docs else 0
        total_buckets = min(total_buckets, buckets)

        timeline_page = paginate(
            page=page,
            page_size=page_size,
            total=total_buckets,
            results=timeline,
        )

        top_paths_data = []
        if top_paths > 0:
            top_paths_pipeline = [
                {"$match": match_stage},
                {"$group": {"_id": "$path", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": top_paths},
                {"$project": {"_id": 0, "path": "$_id", "count": 1}},
            ]
            top_cursor = mongo.stats_data.aggregate(top_paths_pipeline)
            top_paths_data = [doc async for doc in top_cursor]

        recent_requests = []
        if recent_limit > 0:
            recent_cursor = (
                mongo.stats_data.find(
                    match_stage,
                    {
                        "_id": 0,
                        "path": 1,
                        "request_method": 1,
                        "status_code": 1,
                        "remote_address": 1,
                        "created": 1,
                    },
                )
                .sort("created", -1)
                .limit(recent_limit)
            )
            async for doc in recent_cursor:
                created = doc.get("created")
                recent_requests.append(
                    {
                        "path": doc.get("path"),
                        "request_method": doc.get("request_method"),
                        "status_code": doc.get("status_code"),
                        "remote_address": doc.get("remote_address"),
                        "created": created.isoformat() if created else None,
                    }
                )

        total_requests = await mongo.stats_data.count_documents(match_stage)

        return {
            "interval": interval,
            "lookback_minutes": lookback_minutes,
            "since": since.isoformat(),
            "path_filter": path,
            "bucket_count": timeline_page["total"],
            "total_requests": total_requests,
            "timeline": timeline_page,
            "top_paths": top_paths_data,
            "recent_requests": recent_requests,
        }

    cache_key = \
        f"trends:{interval}:{lookback_minutes}:{buckets}:{top_paths}:{recent_limit}:{path}:{page}:{page_size}"

    return await fetch_from_cache(cache_key, loader, ttl=60)
