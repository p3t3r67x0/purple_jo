from fastapi import APIRouter, HTTPException, Query, Depends
from app.api import fetch_latest_dns
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.deps import get_mongo


router = APIRouter()


@router.get("/dns")
async def latest_dns(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo)
):
    items = await fetch_latest_dns(mongo, page=page, page_size=page_size)
    if items and items.get("results"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
