import os
from pathlib import Path

# Paths
DATA_DIR = Path("../data")
MMDB_PATH = DATA_DIR / "GeoLite2-City.mmdb"
ASN_DB = DATA_DIR / "rib.20250901.1600.dat"
ASN_NAMES = DATA_DIR / "asn_names.json"

# Defaults
CACHE_EXPIRE = int(os.getenv("CACHE_EXPIRE", "86400"))
MASSCAN_RATE = int(os.getenv("MASSCAN_RATE", "1000"))
SOCKET_TIMEOUT = int(os.getenv("SOCKET_TIMEOUT", "3"))

# Redis cache configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
REDIS_NAMESPACE = os.getenv("REDIS_NAMESPACE", "purple_jo")

DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "25"))
MAX_PAGE_SIZE = int(os.getenv("MAX_PAGE_SIZE", "200"))


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "ip_data")
