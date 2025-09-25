from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api import fetch_all_prefix
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_mongo

router = APIRouter()


@router.get(
    "/subnet/{sub}/{prefix}",
    tags=["Network Intelligence"],
    summary="Inspect certificate activity for a subnet",
    responses={404: {"description": "Subnet has no recorded certificates"}},
)
async def subnet(
    sub: str,
    prefix: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    """Return certificates observed within the specified subnet and prefix."""

    items = await fetch_all_prefix(
        mongo,
        f"{sub}/{prefix}",
        page=page,
        page_size=page_size,
    )
    if items.get("results"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
