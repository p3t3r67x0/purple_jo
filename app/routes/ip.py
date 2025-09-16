import socket
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.api import fetch_one_ip, asn_lookup
from app.responses import MongoJSONResponse
from app.deps import get_mongo

router = APIRouter()

@router.get("/ip/{ipv4}", response_class=MongoJSONResponse)
async def ip_lookup(
    ipv4: str,
    mongo: AsyncIOMotorDatabase = Depends(get_mongo)
):
    items = await fetch_one_ip(mongo, ipv4)
    if not items:
        res = asn_lookup(ipv4)
        try:
            host = socket.gethostbyaddr(ipv4)[0]
        except Exception:
            host = None

        prop = {
            "ip": ipv4,
            "host": host,
            "updated": datetime.now(),
            "asn": res["asn"],
            "name": res["name"],
            "cidr": [res["prefix"]],
        }

        try:
            await mongo.lookup.insert_one(prop)
        except Exception:
            pass

        items = [prop]

    if items:
        return MongoJSONResponse(items)

    raise HTTPException(status_code=404, detail="No documents found")
