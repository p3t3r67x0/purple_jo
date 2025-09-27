from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from fastapi.responses import JSONResponse

from app.middleware import log_stats
from app.db_postgres import get_engine, get_session_factory, init_db

from app.routes import (
    query,
    subnet,
    match,
    dns,
    cidr,
    ipv4,
    asn,
    graph,
    ip,
    trends,
    live,
    contact,
    admin,
)
from app.settings import get_settings
logger = logging.getLogger("app")


API_DESCRIPTION = (
    "NetScanner exposes network, domain, and certificate intelligence collected "
    "from multiple data sources. Use the REST and WebSocket endpoints to "
    "explore certificate transparency records, inspect IP allocations, follow "
    "real-time scans, and monitor platform usage trends."
)

OPENAPI_TAGS = [
    {
        "name": "Search",
        "description": (
            "Domain-centric lookups and flexible search helpers built on top of "
            "certificate transparency data."
        ),
    },
    {
        "name": "Network Intelligence",
        "description": (
            "IP and subnet focused endpoints that return enriched attribution and "
            "related certificate activity."
        ),
    },
    {
        "name": "Data Feeds",
        "description": (
            "Latest aggregated snapshots for DNS, CIDR, IPv4, and ASN datasets "
            "designed for paginated consumption."
        ),
    },
    {
        "name": "Graph",
        "description": (
            "Relationship graphs that connect domains, certificates, and network "
            "entities for visualization tooling."
        ),
    },
    {
        "name": "Trends",
        "description": (
            "Usage metrics and request analytics that surface activity patterns "
            "over time."
        ),
    },
    {
        "name": "Live Scans",
        "description": (
            "Real-time scanning workflows streamed over WebSocket connections."
        ),
    },
    {
        "name": "Contact",
        "description": "Submit messages to the NetScanner operators via email.",
    },
    {
        "name": "Admin",
        "description": (
            "Privileged operations secured with OAuth2 bearer tokens issued via the admin login."
        ),
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    warnings = settings.recommended_warnings()
    if warnings:
        for w in warnings:
            logger.warning("CONFIG: %s", w)
    try:
        await init_db()
        app.state.postgres_engine = get_engine()
        app.state.postgres_session_factory = get_session_factory()
        logger.info("PostgreSQL engine initialised")
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to initialise PostgreSQL engine: %s", exc)
    yield


app = FastAPI(
    title="NetScanner API",
    description=API_DESCRIPTION,
    version="0.1.12",
    default_response_class=JSONResponse,
    lifespan=lifespan,
    contact={"name": "NetScanner Support", "email": "hello@netscanner.io"},
    license_info={"name": "MIT License",
                  "url": "https://opensource.org/licenses/MIT"},
    openapi_tags=OPENAPI_TAGS,
)

# Middleware
app.middleware("http")(log_stats)

# Routers
app.include_router(query.router)
app.include_router(subnet.router)
app.include_router(match.router)
app.include_router(dns.router)
app.include_router(cidr.router)
app.include_router(ipv4.router)
app.include_router(asn.router)
app.include_router(graph.router)
app.include_router(ip.router)
app.include_router(trends.router)
app.include_router(live.router)
app.include_router(contact.router)
app.include_router(admin.router)
