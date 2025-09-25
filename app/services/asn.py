"""ASN lookup service using pyasn/pyasn2.

Attempts to import `pyasn2` (a hypothetical successor or fork). If it is not
available, falls back transparently to the existing `pyasn` package so the
service continues to operate. This allows a smooth migration path without
breaking runtime environments that still only provide `pyasn`.
"""

try:  # Prefer pyasn2 if installed
    import pyasn2 as pyasn  # type: ignore
    USING_PYASN2 = True
except ImportError:  # Fallback to original library
    import pyasn  # type: ignore
    USING_PYASN2 = False


def asn_lookup(ipv4: str):
    """Lookup ASN and prefix for an IPv4 address.

    Uses whichever backend was successfully imported (`pyasn2` preferred,
    otherwise `pyasn`). Returns a mapping with prefix, organization name, and
    ASN number.
    """
    asndb = pyasn.pyasn(
        'data/rib.20250901.1600.dat',
        as_names_file='data/asn_names.json',
    )
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)
    return {"prefix": prefix, "name": name, "asn": asn, "pyasn2": USING_PYASN2}
