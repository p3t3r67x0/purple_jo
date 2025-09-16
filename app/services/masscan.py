import asyncio
import json


async def run_masscan(ip: str, ports: str = "1-1000", rate: int = 1000) -> list:
    """
    Run masscan against an IP for the given ports, return structured results.
    """
    cmd = [
        "masscan", ip,
        "--ports", ports,
        "--rate", str(rate),
        "-oJ", "-"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            print(f"Masscan error: {stderr.decode().strip()}")
            return []

        try:
            data = json.loads(stdout.decode().strip() or "[]")
        except Exception:
            return []

        results = []
        for entry in data:
            ip_addr = entry.get("ip")
            for p in entry.get("ports", []):
                results.append({
                    "ip": ip_addr,
                    "port": p.get("port"),
                    "proto": p.get("proto"),
                    "status": p.get("status"),
                    "reason": p.get("reason"),
                    "ttl": p.get("ttl"),
                    "service": p.get("service", "")
                })
        return results
    except Exception as e:
        print(f"Masscan execution failed: {e}")
        return []
