from fastapi import APIRouter, HTTPException, Depends
from app.api import fetch_latest_cidr
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.deps import get_mongo

router = APIRouter()


@router.get("/cidr")
async def latest_cidr(mongo: AsyncIOMotorDatabase = Depends(get_mongo)):
    items = await fetch_latest_cidr(mongo)
    if items:
        return items
    raise HTTPException(status_code=404, detail="No documents found")
