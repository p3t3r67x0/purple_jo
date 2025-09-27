from email.message import EmailMessage
import smtplib
from typing import Optional
from datetime import datetime, timedelta, timezone
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
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.deps import get_postgres_session
from app.models.postgres import ContactMessage as ContactMessageModel, ContactRateLimit
from app.settings import get_settings

logger = logging.getLogger("contact")
logger.setLevel(logging.INFO)

router = APIRouter()


class ContactMessageForm(BaseModel):
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
    try:
        ip_obj = ipaddress.ip_address(ip)
    except Exception:  # noqa: BLE001
        return True
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


def _build_email(data: ContactMessageForm) -> EmailMessage:
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


def _verify_token(data: ContactMessageForm):
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


async def _check_rate_limit(session: AsyncSession, ip: str) -> None:
    settings = get_settings()
    limit = settings.contact_rate_limit
    window_seconds = settings.contact_rate_window
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(
        seconds=now.second % window_seconds,
        microseconds=now.microsecond,
    )
    expires_at = window_start + timedelta(seconds=window_seconds)

    stmt = (
        select(ContactRateLimit)
        .where(
            ContactRateLimit.ip_address == ip,
            ContactRateLimit.window_start == window_start,
        )
        .with_for_update()
    )
    result = await session.exec(stmt)
    record = result.scalars().one_or_none()

    if record is None:
        record = ContactRateLimit(
            ip_address=ip,
            window_start=window_start,
            expires_at=expires_at,
            count=1,
        )
        session.add(record)
        await session.flush()
        return

    if record.expires_at <= now:
        record.window_start = window_start
        record.expires_at = expires_at
        record.count = 1
    else:
        record.count += 1
        if record.count > limit:
            await session.rollback()
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Please try again later.",
            )
    await session.flush()


async def _persist_message(
    session: AsyncSession,
    form: ContactMessageForm,
    ip: str,
) -> None:
    message = ContactMessageModel(
        name=form.name,
        email=form.email,
        subject=form.subject,
        message=form.message,
        ip_address=ip,
        created_at=datetime.now(timezone.utc),
    )
    session.add(message)
    await session.flush()


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
    form: ContactMessageForm,
    tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_postgres_session),
):
    """Validate input, enforce rate limits, and queue email delivery."""

    _verify_token(form)

    ip = request.client.host if request.client else "unknown"

    if _ip_blocked(ip):
        logger.warning("Blocked contact submission from %s", ip)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden",
        )

    try:
        await _check_rate_limit(session, ip)
        await _verify_captcha(form.captcha_token, ip)
        await _persist_message(session, form, ip)
        await session.commit()
    except HTTPException:
        await session.rollback()
        raise
    except Exception as exc:  # noqa: BLE001
        await session.rollback()
        logger.exception("Failed to persist contact message")
        raise HTTPException(status_code=500, detail="Unable to store message") from exc

    msg = _build_email(form)
    tasks.add_task(_send_email_async, msg)
    logger.info(
        "contact_submission ip=%s email=%s subject_len=%d",
        ip,
        form.email,
        len(form.subject or ""),
    )
    return {"status": "accepted"}
