from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api import fetch_match_condition
from app.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.deps import get_postgres_session


router = APIRouter()


@router.get(
    "/match/{query:path}",
    tags=["Search"],
    summary="Run an advanced certificate match query",
    responses={404: {"description": "No certificates found for the match criteria"}},
)
async def match(
    query: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    session: AsyncSession = Depends(get_postgres_session),
):
    """Execute flexible match conditions like issuer, organization, or time filters."""

    ql = query.split(":")
    condition = ql[0].lower()

    if condition in ["ipv6", "ca", "crl", "org", "ocsp", "before", "after"]:
        q = ":".join(ql[1:]) if len(ql) > 1 else ""
    else:
        q = ql[1] if len(ql) > 1 else ql[0]

    items = await fetch_match_condition(
        condition,
        q,
        page=page,
        page_size=page_size,
        session=session,
    )
    if items.get("results"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
