from fastapi import APIRouter, HTTPException
from app.api import fetch_all_prefix

router = APIRouter()


@router.get("/subnet/{sub}/{prefix}")
async def subnet(sub: str, prefix: str, mongo=...):
    items = await fetch_all_prefix(mongo, f"{sub}/{prefix}")
    if items:
        return items
    raise HTTPException(status_code=404, detail="No documents found")
