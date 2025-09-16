import ssl
import socket
import re
from datetime import datetime, timezone
from ssl import SSLError


async def extract_certificate(domain: str) -> dict | None:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED

    try:
        with socket.create_connection((domain, 443), timeout=5) as sock:
            with ctx.wrap_socket(sock, server_hostname=domain) as ssock:
                cert = ssock.getpeercert()
    except (ConnectionRefusedError, OSError, SSLError, socket.gaierror, socket.timeout):
        return None

    if not cert:
        return None

    def parse_dn(entries):
        return {"_".join(re.findall(".[^A-Z]*", k)).lower(): v for ((k, v),) in entries}

    return {
        "issuer": parse_dn(cert.get("issuer", [])),
        "subject": parse_dn(cert.get("subject", [])),
        "subject_alt_names": [alt for _, alt in cert.get("subjectAltName", [])],
        "serial": cert.get("serialNumber"),
        "not_before": datetime.strptime(cert["notBefore"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc),
        "not_after": datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc),
        "version": cert.get("version"),
    }
