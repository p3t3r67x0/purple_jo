from datetime import datetime, timedelta
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.deps import get_mongo

router = APIRouter()


def _floor_time(dt: datetime, delta: timedelta) -> datetime:
    """Floor ``dt`` to the start of the ``delta`` sized bucket."""
    if delta <= timedelta(0):
        raise ValueError("delta must be greater than zero")
    return dt - (dt - datetime.min) % delta


@router.get("/stats/trends")
async def get_request_trends(
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
    minutes: int = Query(60, ge=1, le=24 * 60),
    bucket_minutes: int = Query(5, ge=1, le=6 * 60),
    latest_limit: int = Query(20, ge=1, le=200),
    path: Optional[str] = Query(None, description="Filter by request path"),
    method: Optional[str] = Query(None, description="Filter by HTTP method"),
) -> Dict[str, object]:
    """Return a simple time series of the latest API calls.

    The data is backed by the ``stats_data`` collection populated by the
    logging middleware. The endpoint groups requests in ``bucket_minutes``
    buckets over the last ``minutes`` minutes and also returns the latest
    requests that match the same filters.
    """

    if bucket_minutes > minutes:
        raise HTTPException(
            status_code=400,
            detail="bucket_minutes must be less than or equal to minutes",
        )

    now = datetime.now()
    start = now - timedelta(minutes=minutes)
    bucket_delta = timedelta(minutes=bucket_minutes)

    match_filter: Dict[str, object] = {
        "created": {"$gte": start, "$lte": now},
    }

    if path:
        match_filter["path"] = path
    if method:
        match_filter["request_method"] = method.upper()

    bucket_ms = bucket_minutes * 60 * 1000
    pipeline = [
        {"$match": match_filter},
        {
            "$project": {
                "bucket": {
                    "$toDate": {
                        "$subtract": [
                            {"$toLong": "$created"},
                            {"$mod": [
                                {"$toLong": "$created"},
                                bucket_ms,
                            ]},
                        ]
                    }
                }
            }
        },
        {"$group": {"_id": "$bucket", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]

    bucket_counts: Dict[datetime, int] = {}
    async for doc in mongo.stats_data.aggregate(pipeline):
        bucket = doc.get("_id")
        if isinstance(bucket, datetime):
            bucket_counts[bucket] = int(doc.get("count", 0))

    start_bucket = _floor_time(start, bucket_delta)
    end_bucket = _floor_time(now, bucket_delta)

    timeline = []
    current_bucket = start_bucket
    total = 0
    while current_bucket <= end_bucket:
        count = bucket_counts.get(current_bucket, 0)
        total += count
        timeline.append({
            "timestamp": current_bucket,
            "count": count,
        })
        current_bucket += bucket_delta

    latest_filter = match_filter.copy()
    latest_cursor = (
        mongo.stats_data
        .find(latest_filter, {
            "_id": 0,
            "path": 1,
            "query": 1,
            "request_method": 1,
            "status_code": 1,
            "created": 1,
            "remote_address": 1,
        })
        .sort("created", -1)
        .limit(latest_limit)
    )
    latest_requests = [
        {
            "path": doc.get("path"),
            "query": doc.get("query"),
            "request_method": doc.get("request_method"),
            "status_code": doc.get("status_code"),
            "created": doc.get("created"),
            "remote_address": doc.get("remote_address"),
        }
        async for doc in latest_cursor
    ]

    return {
        "start": start_bucket,
        "end": end_bucket,
        "bucket_minutes": bucket_minutes,
        "total_requests": total,
        "timeline": timeline,
        "latest_requests": latest_requests,
    }
