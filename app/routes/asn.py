from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api import fetch_latest_asn
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_mongo

router = APIRouter()


@router.get(
    "/asn",
    tags=["Data Feeds"],
    summary="Enumerate recent ASN intelligence",
    responses={404: {"description": "No ASN records match the supplied filters"}},
)
async def latest_asn(
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    country_code: Optional[str] = Query(None),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    """Return a paginated list of autonomous systems, optionally filtered by country."""

    items = await fetch_latest_asn(
        mongo,
        page=page,
        page_size=page_size,
        country_code=country_code,
    )
    if items.get("results"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
