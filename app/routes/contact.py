from email.message import EmailMessage
import smtplib
from typing import Optional
from datetime import datetime, timedelta
import logging
import httpx
import ipaddress

from fastapi import (
    APIRouter,
    BackgroundTasks,
    HTTPException,
    Depends,
    Request,
    status,
)
from pydantic import BaseModel, EmailStr, constr
import aiosmtplib
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.deps import get_mongo
from app.settings import get_settings

logger = logging.getLogger("contact")
logger.setLevel(logging.INFO)

router = APIRouter()


class ContactMessage(BaseModel):
    name: constr(
        strip_whitespace=True,
        min_length=1,
        max_length=100,
    )  # type: ignore
    email: EmailStr
    subject: constr(
        strip_whitespace=True,
        min_length=1,
        max_length=150,
    )  # type: ignore
    message: constr(
        strip_whitespace=True,
        min_length=5,
        max_length=5000,
    )  # type: ignore
    token: Optional[str] = None  # shared-secret (deprecated if captcha set)
    captcha_token: Optional[str] = None  # hCaptcha/reCAPTCHA response token


def _parse_ip_denylist() -> set[str]:
    raw = (get_settings().contact_ip_denylist or "").strip()
    if not raw:
        return set()
    parts = {p.strip() for p in raw.split(",") if p.strip()}
    return parts


def _ip_blocked(ip: str) -> bool:
    deny = _parse_ip_denylist()
    if not deny:
        return False
    # Support single IPs and CIDR ranges
    try:
        ip_obj = ipaddress.ip_address(ip)
    except Exception:  # noqa: BLE001
        return True  # Malformed -> block for safety
    for entry in deny:
        try:
            if "/" in entry:
                net = ipaddress.ip_network(entry, strict=False)
                if ip_obj in net:
                    return True
            else:
                if ip == entry:
                    return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _build_email(data: ContactMessage) -> EmailMessage:
    msg = EmailMessage()
    s = get_settings()
    msg["From"] = s.contact_from or data.email
    msg["To"] = s.contact_to
    msg["Subject"] = (
        f"[Contact] {data.subject}" if data.subject else "[Contact] Message"
    )

    body = (
        f"Name: {data.name}\n"
        f"Email: {data.email}\n"
        f"Subject: {data.subject}\n\n"
        f"Message:\n{data.message}\n"
    )
    msg.set_content(body)
    return msg


async def _send_email_async(msg: EmailMessage):
    """Send email asynchronously using aiosmtplib, fallback to smtplib."""
    s = get_settings()
    host = s.smtp_host
    port = s.smtp_port
    user: Optional[str] = s.smtp_user
    password: Optional[str] = s.smtp_password
    use_tls = s.smtp_starttls

    try:
        if use_tls:
            await aiosmtplib.send(
                msg,
                hostname=host,
                port=port,
                start_tls=True,
                username=user,
                password=password,
                timeout=10,
            )
        else:
            await aiosmtplib.send(
                msg,
                hostname=host,
                port=port,
                username=user,
                password=password,
                timeout=10,
            )
    except Exception:  # noqa: BLE001 - fallback
        try:
            with smtplib.SMTP(host, port, timeout=10) as server:
                server.ehlo()
                if use_tls:
                    server.starttls()
                    server.ehlo()
                if user and password:
                    server.login(user, password)
                server.send_message(msg)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=500,
                detail=f"Failed to send message: {exc}",
            ) from exc


_rate_index_created = False
_message_index_created = False


async def _ensure_indexes(mongo: AsyncIOMotorDatabase):
    global _rate_index_created, _message_index_created
    if not _rate_index_created:
        try:
            # TTL index for rate limit windows
            await mongo.contact_rate_limit.create_index(
                "expiresAt", expireAfterSeconds=0
            )
        except Exception:  # noqa: BLE001
            pass
        _rate_index_created = True
    if not _message_index_created:
        try:
            await mongo.contact_messages.create_index("created_at")
            await mongo.contact_messages.create_index("ip")
        except Exception:  # noqa: BLE001
            pass
        _message_index_created = True


async def _check_rate_limit(
    mongo: AsyncIOMotorDatabase, ip: str
) -> None:
    s = get_settings()
    limit = s.contact_rate_limit
    window = s.contact_rate_window  # seconds
    now = datetime.utcnow()
    window_start = now - timedelta(
        seconds=now.second % window,
        microseconds=now.microsecond,
    )
    expires = window_start + timedelta(seconds=window)

    key = {"ip": ip, "window_start": window_start}
    update = {
        "$inc": {"count": 1},
        "$setOnInsert": {"expiresAt": expires},
    }
    doc = await mongo.contact_rate_limit.find_one_and_update(
        key,
        update,
        upsert=True,
        return_document=True,
    )
    if doc and doc.get("count", 0) > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please try again later.",
        )


def _verify_token(data: ContactMessage):
    # Token check only if captcha not configured
    s = get_settings()
    if s.hcaptcha_secret or s.recaptcha_secret:
        return
    expected = s.contact_token
    if expected and data.token != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


async def _verify_captcha(captcha_token: Optional[str], ip: str):
    s = get_settings()
    if not captcha_token:
        # If a captcha secret is configured, token must be supplied
        if s.hcaptcha_secret or s.recaptcha_secret:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="captcha_token required",
            )
        return
    h_secret = s.hcaptcha_secret
    r_secret = s.recaptcha_secret
    timeout = httpx.Timeout(5.0)

    try:
        if h_secret:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    "https://hcaptcha.com/siteverify",
                    data={
                        "secret": h_secret,
                        "response": captcha_token,
                        "remoteip": ip,
                    },
                )
            data = resp.json()
            if not data.get("success"):
                logger.warning("hCaptcha failed: %s", data)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid captcha",
                )
        elif r_secret:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    "https://www.google.com/recaptcha/api/siteverify",
                    data={
                        "secret": r_secret,
                        "response": captcha_token,
                        "remoteip": ip,
                    },
                )
            data = resp.json()
            if not data.get("success"):
                logger.warning("reCAPTCHA failed: %s", data)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid captcha",
                )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error("Captcha verification error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Captcha verification error",
        ) from exc


async def _persist_message(
    mongo: AsyncIOMotorDatabase,
    form: ContactMessage,
    ip: str,
    user_agent: Optional[str],
):
    doc = {
        "name": form.name,
        "email": form.email,
        "subject": form.subject,
        "message": form.message,
        "ip": ip,
        "user_agent": user_agent,
        "created_at": datetime.utcnow(),
        "has_token": bool(form.token),
        "has_captcha": bool(form.captcha_token),
    }
    try:
        await mongo.contact_messages.insert_one(doc)
    except Exception:  # noqa: BLE001
        pass  # Do not block on persistence failure


@router.post(
    "/contact",
    status_code=202,
    tags=["Contact"],
    summary="Submit a contact form message",
    responses={
        401: {"description": "Token or captcha validation failed"},
        403: {"description": "IP address is blocked"},
        429: {"description": "Too many requests from this IP"},
        502: {"description": "Captcha verification error"},
        500: {"description": "Delivery to the configured inbox failed"},
    },
)
async def submit_contact(
    request: Request,
    form: ContactMessage,
    tasks: BackgroundTasks,
    mongo: AsyncIOMotorDatabase = Depends(get_mongo),
):
    """Validate input, enforce rate limits, and queue email delivery for the contact form."""
    await _ensure_indexes(mongo)
    _verify_token(form)

    # Client metadata
    ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent")

    if _ip_blocked(ip):
        logger.warning("Blocked contact submission from %s", ip)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden",
        )

    await _check_rate_limit(mongo, ip)
    await _verify_captcha(form.captcha_token, ip)
    await _persist_message(mongo, form, ip, user_agent)

    msg = _build_email(form)
    tasks.add_task(_send_email_async, msg)
    logger.info(
        "contact_submission ip=%s email=%s subject_len=%d ua_len=%d",
        ip,
        form.email,
        len(form.subject or ""),
        len(user_agent or ""),
    )
    return {"status": "accepted"}
