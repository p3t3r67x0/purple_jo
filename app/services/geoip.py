import asyncio
import logging
from pathlib import Path
from typing import Optional

import geoip2.database


logger = logging.getLogger("geoip")

MMDB_PATH = Path("data/GeoLite2-City.mmdb")
_geo_reader: Optional[geoip2.database.Reader] = None

if MMDB_PATH.exists():
    try:
        _geo_reader = geoip2.database.Reader(str(MMDB_PATH))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to open GeoIP database %s: %s", MMDB_PATH, exc)
        _geo_reader = None
else:
    logger.info(
        "GeoIP database not found at %s; returning empty geo lookups",
        MMDB_PATH,
    )


async def fetch_geoip(ip: str) -> dict:
    """
    Lookup GeoIP data for an IP address using MaxMind.
    Returns city, country, state, and geo coordinates.
    """
    try:
        if _geo_reader is None:
            raise RuntimeError("GeoIP database unavailable")

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, lambda: _geo_reader.city(ip)
        )
        return {
            "city": response.city.name,
            "country": response.country.name,
            "country_code": response.country.iso_code,
            "state": response.subdivisions.most_specific.name,
            "loc": {
                "type": "Point",
                "coordinates": [
                    response.location.longitude,
                    response.location.latitude,
                ],
            },
        }
    except Exception:
        return {
            "city": None,
            "country": None,
            "country_code": None,
            "state": None,
            "loc": {},
        }
