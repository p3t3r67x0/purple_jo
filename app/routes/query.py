from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.api import fetch_query_domain
from app.deps import get_mongo

router = APIRouter()


@router.get("/query/{domain}")
async def query_domain(
    domain: str,
    mongo: AsyncIOMotorDatabase = Depends(get_mongo)
):
    items = await fetch_query_domain(mongo, domain)
    if items:
        return items
    raise HTTPException(status_code=404, detail="No documents found")
