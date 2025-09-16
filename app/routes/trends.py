from typing import Literal, Optional

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api import fetch_request_trends
from app.deps import get_mongo


router = APIRouter()


@router.get("/trends/requests")
async def request_trends(
    interval: Literal["minute", "hour", "day"] = Query("minute"),
    lookback_minutes: int = Query(60, gt=0, le=7 * 24 * 60),
    buckets: int = Query(20, gt=0, le=500),
    top_paths: int = Query(5, ge=0, le=50),
    recent_limit: int = Query(20, ge=0, le=100),
    path: Optional[str] = Query(None),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    """Return aggregated request trends for the API."""

    return await fetch_request_trends(
        mongo,
        interval=interval,
        lookback_minutes=lookback_minutes,
        buckets=buckets,
        top_paths=top_paths,
        recent_limit=recent_limit,
        path=path,
    )
