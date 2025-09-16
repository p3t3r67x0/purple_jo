import asyncio
import geoip2.database

MMDB_PATH = "./data/GeoLite2-City.mmdb"
geo_reader = geoip2.database.Reader(MMDB_PATH)


async def fetch_geoip(ip: str) -> dict:
    """
    Lookup GeoIP data for an IP address using MaxMind.
    Returns city, country, state, and geo coordinates.
    """
    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: geo_reader.city(ip))
        return {
            "city": response.city.name,
            "country": response.country.name,
            "country_code": response.country.iso_code,
            "state": response.subdivisions.most_specific.name,
            "loc": {
                "type": "Point",
                "coordinates": [response.location.longitude, response.location.latitude],
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
