from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api import extract_graph
from app.deps import get_mongo


router = APIRouter()


@router.get(
    "/graph/{site}",
    tags=["Graph"],
    summary="Build a relationship graph for a domain",
    responses={404: {"description": "No graph nodes were generated for the domain"}},
)
async def graph(
    site: str,
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    """Generate a network graph showing entities connected to the requested site."""

    items = await extract_graph(mongo, site)
    if items.get("nodes"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
