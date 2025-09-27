from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api import fetch_latest_dns
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_postgres_session


router = APIRouter()


@router.get(
    "/dns",
    tags=["Data Feeds"],
    summary="Retrieve the latest DNS observations",
    responses={404: {"description": "No DNS records available for the requested page"}},
)
async def latest_dns(
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    session: AsyncSession = Depends(get_postgres_session),
):
    """Return a paginated feed of the most recent DNS records processed by NetScanner."""

    items = await fetch_latest_dns(
        page=page,
        page_size=page_size,
        session=session,
    )
    if items.get("results"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
