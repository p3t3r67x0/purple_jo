from fastapi import APIRouter, HTTPException, Depends
from app.api import fetch_latest_ipv4
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.deps import get_mongo


router = APIRouter()


@router.get("/ipv4")
async def latest_ipv4(mongo: AsyncIOMotorDatabase = Depends(get_mongo)):
    items = await fetch_latest_ipv4(mongo)
    if items and items.get("results"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
