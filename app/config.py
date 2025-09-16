from pathlib import Path

# Paths
DATA_DIR = Path("../data")
MMDB_PATH = DATA_DIR / "GeoLite2-City.mmdb"
ASN_DB = DATA_DIR / "rib.20250901.1600.dat"
ASN_NAMES = DATA_DIR / "asn_names.json"

# Defaults
CACHE_EXPIRE = 86400
MASSCAN_RATE = 1000
SOCKET_TIMEOUT = 3


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ip_data"