async def fetch_dns_records(domain: str) -> dict:
    """
    Fetch DNS records using aiodns (A, AAAA, MX, NS, SOA, etc.).
    """
    import aiodns
    resolver = aiodns.DNSResolver()
    records = {}

    try:
        a_record = await resolver.query(domain, "A")
        records["a_record"] = [r.host for r in a_record]
    except Exception:
        pass

    try:
        aaaa_record = await resolver.query(domain, "AAAA")
        records["aaaa_record"] = [r.host for r in aaaa_record]
    except Exception:
        pass

    try:
        mx_record = await resolver.query(domain, "MX")
        records["mx_record"] = [
            {"preference": r.priority, "exchange": r.host}
            for r in mx_record
        ]
    except Exception:
        pass

    try:
        ns_record = await resolver.query(domain, "NS")
        records["ns_record"] = [r.host for r in ns_record]
    except Exception:
        pass

    try:
        soa_record = await resolver.query(domain, "SOA")
        records["soa_record"] = [str(r) for r in soa_record]
    except Exception:
        pass

    try:
        txt_record = await resolver.query(domain, "TXT")
        records["txt_record"] = [r.text for r in txt_record]
    except Exception:
        pass
    
    try:
        cname_record = await resolver.query(domain, "CNAME")
        records["cname_record"] = [r.host for r in cname_record]
    except Exception:
        pass

    return records
