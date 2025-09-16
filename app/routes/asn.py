from fastapi import APIRouter, HTTPException, Depends
from app.api import fetch_latest_asn
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.deps import get_mongo

router = APIRouter()

@router.get("/asn")
async def latest_asn(mongo: AsyncIOMotorDatabase = Depends(get_mongo)):
    items = await fetch_latest_asn(mongo)
    if items:
        return items
    raise HTTPException(status_code=404, detail="No documents found")
