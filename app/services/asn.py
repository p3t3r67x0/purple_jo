import pyasn


def asn_lookup(ipv4: str):
    """
    Lookup ASN and prefix for an IPv4 using pyasn.
    """
    asndb = pyasn.pyasn('data/rib.20250901.1600.dat',
                        as_names_file='data/asn_names.json')
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)
    return {"prefix": prefix, "name": name, "asn": asn}
