from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api import fetch_one_ip
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_mongo
from app.responses import MongoJSONResponse

router = APIRouter()


@router.get(
    "/ip/{ipv4}",
    response_class=MongoJSONResponse,
    tags=["Network Intelligence"],
    summary="Inspect a single IPv4 address",
    responses={404: {"description": "The IPv4 address has no recorded activity"}},
)
async def ip_lookup(
    ipv4: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    """Return certificate observations and context for an IPv4 address."""

    items = await fetch_one_ip(mongo, ipv4, page=page, page_size=page_size)
    if items.get("results"):
        return items

    raise HTTPException(status_code=404, detail="No documents found")
