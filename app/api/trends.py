"""Helpers for aggregating request statistics stored in PostgreSQL."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Literal, Optional, Tuple

from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.utils import paginate
from app.cache import fetch_from_cache
from app.models.postgres import RequestStat

_DATE_BUCKETS: Dict[Literal["minute", "hour", "day"], timedelta] = {
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
}


def _pagination_bounds(page: int, page_size: int) -> Tuple[int, int]:
    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    return (safe_page - 1) * safe_page_size, safe_page_size


async def fetch_request_trends(
    *,
    session: AsyncSession,
    interval: Literal["minute", "hour", "day"],
    lookback_minutes: int,
    buckets: int,
    top_paths: int,
    recent_limit: int,
    path: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
) -> dict:
    """Aggregate request statistics for the trend endpoint."""

    bucket_delta = _DATE_BUCKETS[interval]
    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    filters = [RequestStat.created_at >= since]
    if path:
        filters.append(RequestStat.path == path)

    async def loader() -> dict:
        bucket_column = func.date_trunc(interval, RequestStat.created_at)
        timeline_stmt = (
            select(bucket_column.label("bucket"), func.count().label("count"))
            .where(*filters)
            .group_by("bucket")
            .order_by("bucket")
        )
        timeline_result = await session.exec(timeline_stmt)
        timeline_rows = timeline_result.all()
        if buckets > 0:
            timeline_rows = timeline_rows[-buckets:]

        total_buckets = len(timeline_rows)
        skip, limit = _pagination_bounds(page, page_size)
        page_rows = timeline_rows[skip: skip + limit]

        timeline = []
        for bucket_value, count in page_rows:
            bucket_start = bucket_value.replace(tzinfo=timezone.utc)
            timeline.append(
                {
                    "window_start": bucket_start.isoformat(),
                    "window_end": (bucket_start + bucket_delta).isoformat(),
                    "count": count,
                }
            )

        timeline_page = paginate(
            page=page,
            page_size=page_size,
            total=total_buckets,
            results=timeline,
        )

        top_paths_data = []
        if top_paths > 0:
            top_stmt = (
                select(RequestStat.path, func.count().label("count"))
                .where(*filters)
                .group_by(RequestStat.path)
                .order_by(func.count().desc())
                .limit(top_paths)
            )
            top_result = await session.exec(top_stmt)
            for path_value, count in top_result.all():
                top_paths_data.append({"path": path_value, "count": count})

        recent_requests = []
        if recent_limit > 0:
            recent_stmt = (
                select(RequestStat)
                .where(*filters)
                .order_by(RequestStat.created_at.desc())
                .limit(recent_limit)
            )
            recent_result = await session.exec(recent_stmt)
            for stat in recent_result.all():
                recent_requests.append(
                    {
                        "path": stat.path,
                        "request_method": stat.request_method,
                        "status_code": stat.status_code,
                        "remote_address": stat.remote_address,
                        "created": stat.created_at.replace(tzinfo=timezone.utc).isoformat()
                        if stat.created_at
                        else None,
                    }
                )

        total_requests_stmt = select(func.count()).where(*filters)
        total_requests = await session.scalar(total_requests_stmt) or 0

        return {
            "interval": interval,
            "lookback_minutes": lookback_minutes,
            "since": since.isoformat(),
            "path_filter": path,
            "bucket_count": timeline_page["total"],
            "total_requests": int(total_requests),
            "timeline": timeline_page,
            "top_paths": top_paths_data,
            "recent_requests": recent_requests,
        }

    cache_key = (
        f"trends:{interval}:{lookback_minutes}:{buckets}:{top_paths}:{recent_limit}:{path}:{page}:{page_size}"
    )

    return await fetch_from_cache(cache_key, loader, ttl=60)
