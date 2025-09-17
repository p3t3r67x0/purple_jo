from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api import fetch_latest_ipv4
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_mongo


router = APIRouter()


@router.get("/ipv4")
async def latest_ipv4(
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    items = await fetch_latest_ipv4(mongo, page=page, page_size=page_size)
    if items.get("results"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
