from fastapi import APIRouter, HTTPException, Depends
from app.api import extract_graph
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.deps import get_mongo


router = APIRouter()


@router.get("/graph/{site}")
async def graph(site: str, mongo: AsyncIOMotorDatabase = Depends(get_mongo)):
    items = await extract_graph(mongo, site)
    if items:
        return items
    raise HTTPException(status_code=404, detail="No documents found")
