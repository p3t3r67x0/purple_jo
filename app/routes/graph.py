from fastapi import APIRouter, Depends, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api import extract_graph
from app.deps import get_postgres_session


router = APIRouter()


@router.get(
    "/graph/{site}",
    tags=["Graph"],
    summary="Build a relationship graph for a domain",
    responses={404: {"description": "No graph nodes were generated for the domain"}},
)
async def graph(
    site: str,
    session: AsyncSession = Depends(get_postgres_session),
):
    """Generate a network graph showing entities connected to the requested site."""

    items = await extract_graph(site, session=session)
    if items.get("nodes"):
        return items
    raise HTTPException(status_code=404, detail="No documents found")
