import logging
from datetime import datetime, timezone
from ipaddress import IPv4Address, IPv6Address, ip_address, ip_network

from fastapi import Request, Response

from app.db_postgres import get_session_factory
from app.models.postgres import RequestStat

logger = logging.getLogger(__name__)


async def log_stats(request: Request, call_next):
    response: Response = await call_next(request)

    headers = request.headers
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    remote_address = headers.get("x-forwarded-for") or (
        request.client.host if request.client else None
    )

    stats_kwargs = {
        "origin": headers.get("origin"),
        "accept": headers.get("accept"),
        "referer": headers.get("referer"),
        "connection": headers.get("connection"),
        "user_agent": headers.get("user-agent"),
        "accept_language": headers.get("accept-language"),
        "accept_encoding": headers.get("accept-encoding"),
        "remote_address": _anonymize_remote_address(remote_address),
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
    except Exception:  # pragma: no cover - best effort
        logger.exception("Failed to log stats")

    return response


def _anonymize_remote_address(raw_value: str | None) -> str | None:
    """Return an anonymised representation of the client address."""
    if not raw_value:
        return None

    parts = [part.strip() for part in raw_value.split(",") if part.strip()]
    anonymised_parts: list[str] = []

    for part in parts:
        anonymised = _anonymize_single_address(part)
        if anonymised:
            anonymised_parts.append(anonymised)

    if not anonymised_parts:
        return None

    return ", ".join(anonymised_parts)


def _anonymize_single_address(value: str) -> str | None:
    """Mask a single IP address by truncating it to a coarse network boundary."""
    address = value

    if address.startswith("[") and "]" in address:
        closing_index = address.find("]")
        address = address[1:closing_index]
    elif ":" in address and address.count(":") == 1:
        host, possible_port = address.rsplit(":", 1)
        if possible_port.isdigit():
            address = host

    try:
        parsed = ip_address(address)
    except ValueError:
        return None

    network = ip_network(f"{parsed}/{_network_prefix(parsed)}", strict=False)
    return str(network.network_address)


def _network_prefix(parsed_address: IPv4Address | IPv6Address) -> int:
    if isinstance(parsed_address, IPv4Address):
        return 24

    return 64
