from typing import Literal, Optional

from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api import fetch_request_trends
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_postgres_session


router = APIRouter()


@router.get(
    "/trends/requests",
    tags=["Trends"],
    summary="Analyze recent API request trends",
)
async def request_trends(
    interval: Literal["minute", "hour", "day"] = Query("minute"),
    lookback_minutes: int = Query(60, gt=0, le=7 * 24 * 60),
    buckets: int = Query(20, gt=0, le=500),
    top_paths: int = Query(5, ge=0, le=50),
    recent_limit: int = Query(20, ge=0, le=100),
    path: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    session: AsyncSession = Depends(get_postgres_session),
):
    """Return aggregated request metrics including histograms, top paths, and recent entries."""

    return await fetch_request_trends(
        interval=interval,
        lookback_minutes=lookback_minutes,
        buckets=buckets,
        top_paths=top_paths,
        recent_limit=recent_limit,
        path=path,
        page=page,
        page_size=page_size,
        session=session,
    )
