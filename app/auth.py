from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from typing import Optional, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

PASSWORD_HASH_ITERATIONS = 120_000
TOKEN_TTL_MINUTES = 60
TOKEN_BYTES = 48


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    encoded_salt = base64.urlsafe_b64encode(salt).decode("utf-8")
    return f"{encoded_salt}${digest.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        encoded_salt, stored_digest = stored_hash.split("$", 1)
        salt = base64.urlsafe_b64decode(encoded_salt.encode("utf-8"))
    except Exception:  # noqa: BLE001
        return False

    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return hmac.compare_digest(digest.hex(), stored_digest)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def create_access_token(
    mongo: AsyncIOMotorDatabase,
    user_id: ObjectId,
    ttl_minutes: int = TOKEN_TTL_MINUTES,
) -> Tuple[str, datetime]:
    token = secrets.token_urlsafe(TOKEN_BYTES)
    token_hash = _hash_token(token)
    now = datetime.utcnow()
    expires_at = now + timedelta(minutes=ttl_minutes)

    await mongo.admin_tokens.insert_one(
        {
            "user_id": user_id,
            "token_hash": token_hash,
            "created_at": now,
            "expires_at": expires_at,
        }
    )

    return token, expires_at


async def get_active_token(
    mongo: AsyncIOMotorDatabase,
    token: str,
) -> Optional[dict]:
    token_hash = _hash_token(token)
    token_doc = await mongo.admin_tokens.find_one({"token_hash": token_hash})
    if not token_doc:
        return None

    expires_at = token_doc.get("expires_at")
    if expires_at and expires_at < datetime.utcnow():
        await mongo.admin_tokens.delete_one({"_id": token_doc["_id"]})
        return None

    return token_doc
