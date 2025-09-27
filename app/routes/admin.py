from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.auth import (
    create_access_token,
    get_active_token,
    hash_password,
    verify_password,
)
from app.deps import get_postgres_session
from app.models.postgres import AdminUser, ContactMessage

router = APIRouter(prefix="/admin", tags=["Admin"])  # Simple prefix

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="admin/token")


class AdminSignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    full_name: Optional[str] = None


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class AdminProfile(BaseModel):
    id: str
    email: EmailStr
    full_name: Optional[str] = None


class AdminPasswordGrantForm:
    """Lenient password grant parser that surfaces clearer errors."""

    def __init__(
        self,
        grant_type: str = Form(default="password"),
        username: str = Form(...),
        password: str = Form(...),
        scope: str = Form(default=""),
        client_id: Optional[str] = Form(default=None),
        client_secret: Optional[str] = Form(default=None),
    ) -> None:
        self.grant_type = grant_type
        self.username = username
        self.password = password
        self.scopes = scope.split()
        self.client_id = client_id
        self.client_secret = client_secret


async def get_current_admin(
    token: str = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_postgres_session),
) -> AdminProfile:
    token_row = await get_active_token(session, token)
    if token_row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
        )

    user = token_row.user
    if user is None:
        stmt = select(AdminUser).where(AdminUser.id == token_row.user_id)
        result = await session.exec(stmt)
        user = result.scalars().one_or_none()
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Admin no longer exists",
            )

    return AdminProfile(
        id=str(user.id),
        email=user.email,
        full_name=user.full_name,
    )


@router.post(
    "/signup",
    status_code=status.HTTP_201_CREATED,
    summary="Create a new admin account",
)
async def signup_admin(
    payload: AdminSignupRequest,
    session: AsyncSession = Depends(get_postgres_session),
):
    email = payload.email.lower()
    stmt = select(AdminUser).where(func.lower(AdminUser.email) == email)
    result = await session.exec(stmt)
    existing = result.scalars().one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Admin with this email already exists",
        )

    password_hash = hash_password(payload.password)
    now = datetime.now(timezone.utc)
    admin_user = AdminUser(
        email=email,
        password_hash=password_hash,
        full_name=payload.full_name,
        created_at=now,
        updated_at=now,
    )
    session.add(admin_user)
    await session.commit()
    await session.refresh(admin_user)

    return {
        "id": str(admin_user.id),
        "email": admin_user.email,
        "full_name": admin_user.full_name,
    }


@router.post(
    "/token",
    response_model=TokenResponse,
    summary="Obtain an OAuth2 access token",
)
async def login_admin(
    form_data: AdminPasswordGrantForm = Depends(),
    session: AsyncSession = Depends(get_postgres_session),
):
    if form_data.grant_type not in ("", "password", None):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported grant type; use 'password'",
        )

    email = form_data.username.lower()
    stmt = select(AdminUser).where(func.lower(AdminUser.email) == email)
    result = await session.exec(stmt)
    user = result.scalars().one_or_none()
    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect username or password",
        )

    token, expires_at = await create_access_token(session, user.id)
    expires_in = max(0, int((expires_at - datetime.now(timezone.utc)).total_seconds()))

    return TokenResponse(access_token=token, token_type="bearer", expires_in=expires_in)


@router.get(
    "/contact/messages",
    summary="List recent contact form submissions",
    responses={
        401: {"description": "Missing or invalid credentials"},
    },
)
async def list_contact_messages(
    session: AsyncSession = Depends(get_postgres_session),
    _admin: AdminProfile = Depends(get_current_admin),
    limit: int = Query(50, ge=1, le=500),
    since_minutes: Optional[int] = Query(None, ge=1, le=60 * 24),
):
    """Return the most recent contact form submissions for operational monitoring."""

    stmt = select(ContactMessage)
    if since_minutes:
        since = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        stmt = stmt.where(ContactMessage.created_at >= since)
    stmt = stmt.order_by(ContactMessage.created_at.desc()).limit(limit)

    result = await session.exec(stmt)
    messages = result.scalars().all()
    items = [
        {
            "id": message.id,
            "name": message.name,
            "email": message.email,
            "subject": message.subject,
            "message": message.message,
            "ip_address": message.ip_address,
            "created_at": message.created_at.isoformat()
            if message.created_at
            else None,
        }
        for message in messages
    ]
    return {"count": len(items), "results": items}
