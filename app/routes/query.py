from fastapi import APIRouter, Depends, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api import fetch_query_domain
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_mongo

router = APIRouter()


@router.get(
    "/query/{domain}",
    tags=["Search"],
    summary="Get certificate records for a domain",
    responses={404: {"description": "No matching certificates found"}},
)
async def query_domain(
    domain: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    """Return paginated certificate transparency documents that match the domain."""

    items = await fetch_query_domain(mongo, domain, page=page, page_size=page_size)
    if items.get("results"):
        return items
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                        detail="No documents found")
