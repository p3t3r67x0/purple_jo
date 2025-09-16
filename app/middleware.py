from datetime import datetime
from fastapi import Request, Response


async def log_stats(request: Request, call_next):
    response: Response = await call_next(request)

    stats = {}
    headers = request.headers

    # collect header info
    for key, target in {
        "origin": "origin",
        "accept": "accept",
        "referer": "referer",
        "connection": "connection",
        "user-agent": "user_agent",
        "accept-language": "accept_language",
        "accept-encoding": "accept_encoding",
    }.items():
        if key in headers:
            stats[target] = headers[key]

    stats["remote_address"] = headers.get(
        "x-forwarded-for") or (request.client.host if request.client else None)
    stats["scheme"] = headers.get("x-forwarded-proto", request.url.scheme)
    stats["host"] = headers.get("x-forwarded-host", request.url.hostname)
    stats["port"] = headers.get(
        "x-forwarded-port", str(request.url.port) if request.url.port else None)

    stats.update({
        "path": request.url.path,
        "query": request.url.query,
        "fragment": request.url.fragment,
        "request_method": request.method,
        "status_code": response.status_code,
        "created": datetime.now(),
    })

    try:
        await request.app.state.mongo.stats_data.insert_one(stats)
    except Exception as e:
        print(f"Failed to log stats: {e}")

    return response
