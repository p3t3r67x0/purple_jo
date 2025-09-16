import pyqrcode


def generate_qrcode(domain: str) -> dict:
    """
    Generate a QR code PNG (base64 encoded) for a given domain.
    """
    url = pyqrcode.create(f"https://{domain}", encoding="utf-8")
    qrcode_b64 = url.png_as_base64_str(scale=5, quiet_zone=0)
    return {"qrcode": qrcode_b64}
