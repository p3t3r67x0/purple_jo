from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api import fetch_query_domain
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_postgres_session

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
    session: AsyncSession = Depends(get_postgres_session),
):
    """Return paginated certificate transparency documents that match the domain."""

    items = await fetch_query_domain(
        domain,
        page=page,
        page_size=page_size,
        session=session,
    )
    if items.get("results"):
        return items
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                        detail="No documents found")
