# app/services/whois.py
import ipwhois


async def fetch_asn_whois(ip: str) -> dict:
    """Fetch ASN WHOIS info for an IP address."""
    try:
        obj = ipwhois.IPWhois(ip)
        res = obj.lookup_rdap()
        return {
            "asn": res.get("asn"),
            "asn_registry": res.get("asn_registry"),
            "asn_cidr": res.get("asn_cidr"),
            "asn_country_code": res.get("asn_country_code"),
            "asn_date": res.get("asn_date"),
            "asn_description": res.get("asn_description"),
        }
    except Exception:
        return {}


def asn_lookup(ip: str) -> dict:
    """Lightweight ASN lookup (sync, used for IP fallback)."""
    try:
        obj = ipwhois.IPWhois(ip)
        res = obj.lookup_rdap()
        return {
            "asn": res.get("asn"),
            "prefix": res.get("asn_cidr"),
            "name": res.get("asn_description"),
        }
    except Exception:
        return {"asn": None, "prefix": None, "name": None}
