from datetime import datetime, timezone

from fastapi import Request, Response

from app.db_postgres import get_session_factory
from app.models.postgres import RequestStat


async def log_stats(request: Request, call_next):
    response: Response = await call_next(request)

    headers = request.headers
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    stats_kwargs = {
        "origin": headers.get("origin"),
        "accept": headers.get("accept"),
        "referer": headers.get("referer"),
        "connection": headers.get("connection"),
        "user_agent": headers.get("user-agent"),
        "accept_language": headers.get("accept-language"),
        "accept_encoding": headers.get("accept-encoding"),
        "remote_address": headers.get("x-forwarded-for")
        or (request.client.host if request.client else None),
        "scheme": headers.get("x-forwarded-proto", request.url.scheme),
        "host": headers.get("x-forwarded-host", request.url.hostname),
        "port": headers.get("x-forwarded-port", str(request.url.port) if request.url.port else None),
        "path": request.url.path,
        "query": request.url.query,
        "fragment": request.url.fragment,
        "request_method": request.method,
        "status_code": response.status_code,
        "created_at": created_at,
    }

    session_factory = getattr(request.app.state, "postgres_session_factory", None)
    if session_factory is None:
        session_factory = get_session_factory()

    try:
        async with session_factory() as session:
            record = RequestStat(**stats_kwargs)
            session.add(record)
            await session.commit()
    except Exception as exc:  # pragma: no cover - best effort
        print(f"Failed to log stats: {exc}")

    return response
