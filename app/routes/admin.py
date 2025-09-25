from datetime import datetime, timedelta
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, Form, HTTPException, Query, status
from fastapi.security import OAuth2PasswordBearer
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, EmailStr, Field

from app.auth import (
    create_access_token,
    get_active_token,
    hash_password,
    verify_password,
)
from app.deps import get_mongo

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
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
) -> AdminProfile:
    token_doc = await get_active_token(mongo, token)
    if not token_doc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
        )

    user_id = token_doc.get("user_id")
    if not isinstance(user_id, ObjectId):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
        )

    user = await mongo.admin_users.find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin no longer exists",
        )

    return AdminProfile(
        id=str(user["_id"]),
        email=user["email"],
        full_name=user.get("full_name"),
    )


@router.post(
    "/signup",
    status_code=status.HTTP_201_CREATED,
    summary="Create a new admin account",
)
async def signup_admin(
    payload: AdminSignupRequest,
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    email = payload.email.lower()
    existing = await mongo.admin_users.find_one({"email": email})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Admin with this email already exists",
        )

    password_hash = hash_password(payload.password)
    now = datetime.utcnow()
    doc = {
        "email": email,
        "password_hash": password_hash,
        "full_name": payload.full_name,
        "created_at": now,
        "updated_at": now,
    }

    result = await mongo.admin_users.insert_one(doc)
    return {
        "id": str(result.inserted_id),
        "email": email,
        "full_name": payload.full_name,
    }


@router.post(
    "/token",
    response_model=TokenResponse,
    summary="Obtain an OAuth2 access token",
)
async def login_admin(
    form_data: AdminPasswordGrantForm = Depends(),
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    if form_data.grant_type not in ("", "password", None):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported grant type; use 'password'",
        )

    email = form_data.username.lower()
    user = await mongo.admin_users.find_one({"email": email})
    if not user or not verify_password(form_data.password, user.get("password_hash", "")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect username or password",
        )

    token, expires_at = await create_access_token(mongo, user["_id"])
    expires_in = max(0, int((expires_at - datetime.utcnow()).total_seconds()))

    return TokenResponse(access_token=token, token_type="bearer", expires_in=expires_in)


@router.get(
    "/contact/messages",
    summary="List recent contact form submissions",
    responses={
        401: {"description": "Missing or invalid credentials"},
    },
)
async def list_contact_messages(
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
    _admin: AdminProfile = Depends(get_current_admin),
    limit: int = Query(50, ge=1, le=500),
    since_minutes: Optional[int] = Query(None, ge=1, le=60 * 24),
):
    """Return the most recent contact form submissions for operational monitoring."""

    query = {}
    if since_minutes:
        query["created_at"] = {
            "$gte": datetime.utcnow() - timedelta(minutes=since_minutes)
        }

    cursor = (
        mongo.contact_messages.find(query, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    results = await cursor.to_list(length=limit)
    return {"count": len(results), "results": results}
