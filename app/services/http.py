import httpx


async def fetch_site_headers(domain: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"http://{domain}", follow_redirects=True)
            return {
                "status": resp.status_code,
                **dict(resp.headers),
            }
    except Exception as e:
        return {"error": str(e)}
