from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.responses import MongoJSONResponse
from app.middleware import log_stats
from app.db import db, recreate_text_index

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
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # await recreate_text_index()
    print("INFO: Recreated text index")
    yield


app = FastAPI(default_response_class=MongoJSONResponse, lifespan=lifespan)

# Middleware
app.middleware("http")(log_stats)

# Mongo connection
app.state.mongo = db

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
