from fastapi import APIRouter, HTTPException, Depends
from app.api import fetch_match_condition
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.deps import get_mongo


router = APIRouter()


@router.get("/match/{query:path}")
async def match(
    query: str,
    mongo: AsyncIOMotorDatabase = Depends(get_mongo)
):
    ql = query.split(":")
    condition = ql[0].lower()

    if condition in ["ipv6", "ca", "crl", "org", "ocsp", "before", "after"]:
        q = ":".join(ql[1:]) if len(ql) > 1 else ""
    else:
        q = ql[1] if len(ql) > 1 else ql[0]

    items = await fetch_match_condition(mongo, condition, q)
    if items:
        return items
    raise HTTPException(status_code=404, detail="No documents found")
