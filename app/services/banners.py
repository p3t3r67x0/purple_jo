import socket


async def grab_banner(ip: str, port: int = 22) -> str | None:
    """Try to grab a banner from IP:port asynchronously."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((ip, port))
        banner = s.recv(1024)
        s.close()
        return banner.decode("utf-8", errors="ignore").strip()
    except Exception:
        return None
