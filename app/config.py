"""Backward-compatible config constants.

This module now sources values from the Pydantic settings model. Existing
imports like `from app import config` continue to work without immediate
refactors. New code should prefer importing `get_settings` from
`app.settings` directly when convenient.
"""

from pathlib import Path
from app.settings import get_settings

_s = get_settings()

# Paths (static paths kept local)
DATA_DIR = Path("../data")
MMDB_PATH = DATA_DIR / "GeoLite2-City.mmdb"
ASN_DB = DATA_DIR / "rib.20250901.1600.dat"
ASN_NAMES = DATA_DIR / "asn_names.json"

# Direct mappings from settings
MONGO_URI = _s.mongo_uri
DB_NAME = _s.db_name

# Legacy general settings
CACHE_EXPIRE = _s.cache_expire
MASSCAN_RATE = _s.masscan_rate
SOCKET_TIMEOUT = _s.socket_timeout

# Redis
REDIS_HOST = _s.redis_host
REDIS_PORT = _s.redis_port
REDIS_DB = _s.redis_db
REDIS_NAMESPACE = _s.redis_namespace
REDIS_PASSWORD = _s.redis_password

# Pagination
DEFAULT_PAGE_SIZE = _s.default_page_size
MAX_PAGE_SIZE = _s.max_page_size

SMTP_HOST = _s.smtp_host
SMTP_PORT = _s.smtp_port
SMTP_USER = _s.smtp_user
SMTP_PASSWORD = _s.smtp_password
SMTP_STARTTLS = _s.smtp_starttls

CONTACT_TO = _s.contact_to
CONTACT_FROM = _s.contact_from
CONTACT_RATE_LIMIT = _s.contact_rate_limit
CONTACT_RATE_WINDOW = _s.contact_rate_window
CONTACT_TOKEN = _s.contact_token

HCAPTCHA_SECRET = _s.hcaptcha_secret
RECAPTCHA_SECRET = _s.recaptcha_secret
CONTACT_IP_DENYLIST = _s.contact_ip_denylist

# Provide namespace parity for potential future additions
__all__ = [
    "DATA_DIR",
    "MMDB_PATH",
    "ASN_DB",
    "ASN_NAMES",
    "MONGO_URI",
    "DB_NAME",
    "CACHE_EXPIRE",
    "MASSCAN_RATE",
    "SOCKET_TIMEOUT",
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_DB",
    "REDIS_NAMESPACE",
    "REDIS_PASSWORD",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "SMTP_HOST",
    "SMTP_PORT",
    "SMTP_USER",
    "SMTP_PASSWORD",
    "SMTP_STARTTLS",
    "CONTACT_TO",
    "CONTACT_FROM",
    "CONTACT_RATE_LIMIT",
    "CONTACT_RATE_WINDOW",
    "CONTACT_TOKEN",
    "HCAPTCHA_SECRET",
    "RECAPTCHA_SECRET",
    "CONTACT_IP_DENYLIST",
]
