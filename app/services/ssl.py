import asyncio
import ssl
import re
from datetime import datetime, timezone
from typing import Any, Dict

from app.config import SOCKET_TIMEOUT


TIMEOUT = SOCKET_TIMEOUT


async def test_tls_version(domain: str, port: int = 443) -> Dict[str, Dict[str, Any]]:
    """Try connecting with specific TLS versions and report which succeed."""
    results: Dict[str, Dict[str, Any]] = {}

    tls_versions = {
        "TLSv1.0": ssl.TLSVersion.TLSv1,
        "TLSv1.1": ssl.TLSVersion.TLSv1_1,
        "TLSv1.2": ssl.TLSVersion.TLSv1_2,
        "TLSv1.3": ssl.TLSVersion.TLSv1_3,
    }

    for label, version in tls_versions.items():
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.minimum_version = version
            ctx.maximum_version = version
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    domain, port, ssl=ctx, server_hostname=domain),
                timeout=TIMEOUT,
            )
            ssl_obj = writer.get_extra_info("ssl_object")
            cipher = None
            selected_version = None
            if ssl_obj:
                cipher_tuple = ssl_obj.cipher()
                if cipher_tuple:
                    cipher = {
                        "name": cipher_tuple[0],
                        "protocol": cipher_tuple[1],
                        "bits": cipher_tuple[2],
                    }
                selected_version = ssl_obj.version()

            results[label] = {
                "accepted": True,
                "cipher": cipher,
                "version": selected_version,
            }
            writer.close()
            await writer.wait_closed()
        except Exception:
            results[label] = {"accepted": False}

    return results


async def extract_certificate(domain: str) -> dict | None:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

    handshake_cipher = None
    handshake_version = None
    shared_ciphers = None

    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(
                domain,
                443,
                ssl=ssl_context,
                server_hostname=domain,
            ),
            timeout=TIMEOUT,
        )
        ssl_obj = writer.get_extra_info("ssl_object")
        cert = writer.get_extra_info("peercert")

        if ssl_obj:
            cipher_tuple = ssl_obj.cipher()
            if cipher_tuple:
                handshake_cipher = {
                    "name": cipher_tuple[0],
                    "protocol": cipher_tuple[1],
                    "bits": cipher_tuple[2],
                }
            handshake_version = ssl_obj.version()
            try:
                shared_raw = ssl_obj.shared_ciphers()
            except (AttributeError, NotImplementedError):
                shared_raw = None
            if shared_raw:
                shared_ciphers = [
                    {"name": name, "protocol": protocol, "bits": bits}
                    for name, protocol, bits in shared_raw
                ]

        writer.close()
        await writer.wait_closed()
    except Exception:
        return None

    if not cert:
        return None

    def parse_dn(entries):
        return {"_".join(re.findall(".[^A-Z]*", k)).lower(): v for ((k, v),) in entries}

    post = {
        "issuer": parse_dn(cert.get("issuer", [])),
        "subject": parse_dn(cert.get("subject", [])),
        "subject_alt_names": [alt for _, alt in cert.get("subjectAltName", [])],
        "serial": cert.get("serialNumber"),
        "not_before": datetime.strptime(cert["notBefore"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc),
        "not_after": datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc),
        "version": cert.get("version"),
        "handshake_version": handshake_version,
        "handshake_cipher": handshake_cipher,
        "shared_ciphers": shared_ciphers,
    }

    if "OCSP" in cert and cert["OCSP"]:
        post["ocsp"] = cert["OCSP"][0].strip("/")

    if "caIssuers" in cert and cert["caIssuers"]:
        post["ca_issuers"] = cert["caIssuers"][0].strip("/")

    if "crlDistributionPoints" in cert and cert["crlDistributionPoints"]:
        post["crl_distribution_points"] = cert["crlDistributionPoints"][0].strip(
            "/")

    post["tls_versions"] = await test_tls_version(domain)

    return post
