from motor.motor_asyncio import AsyncIOMotorDatabase
from app.db import db  # your shared db instance


async def get_mongo() -> AsyncIOMotorDatabase:
    """
    Dependency to inject the MongoDB database into route handlers.
    """
    return db
