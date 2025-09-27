from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from sqlalchemy.orm import selectinload
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.postgres import AdminToken, AdminUser

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
    session: AsyncSession,
    user_id: int,
    ttl_minutes: int = TOKEN_TTL_MINUTES,
) -> Tuple[str, datetime]:
    token = secrets.token_urlsafe(TOKEN_BYTES)
    token_hash = _hash_token(token)
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=ttl_minutes)

    admin_token = AdminToken(
        user_id=user_id,
        token_hash=token_hash,
        created_at=now,
        expires_at=expires_at,
    )
    session.add(admin_token)
    await session.commit()

    return token, expires_at


async def get_active_token(
    session: AsyncSession,
    token: str,
) -> Optional[AdminToken]:
    token_hash = _hash_token(token)
    stmt = (
        select(AdminToken)
        .options(selectinload(AdminToken.user))
        .where(AdminToken.token_hash == token_hash)
        .limit(1)
    )
    result = await session.exec(stmt)
    admin_token = result.scalars().one_or_none()
    if admin_token is None:
        return None

    if admin_token.expires_at and admin_token.expires_at < datetime.now(timezone.utc):
        await session.delete(admin_token)
        await session.commit()
        return None

    return admin_token
