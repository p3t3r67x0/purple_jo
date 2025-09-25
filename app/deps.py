from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db import db  # your shared db instance


async def get_mongo(request: Request) -> AsyncIOMotorDatabase:
    """Return the configured Mongo database, falling back to the default client."""

    mongo_override = getattr(request.app.state, "mongo", None)
    if mongo_override is not None:
        return mongo_override

    return db
