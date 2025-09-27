"""Backward-compatible config constants.

This module now sources values from the Pydantic settings model. Existing
imports like `from app import config` continue to work without immediate
refactors. New code should prefer importing `get_settings` from
`app.settings` directly when convenient.
"""

from pathlib import Path


# Paths (static paths kept local)
DATA_DIR = Path("../data")
MMDB_PATH = DATA_DIR / "GeoLite2-City.mmdb"
ASN_DB = DATA_DIR / "rib.20250901.1600.dat"
ASN_NAMES = DATA_DIR / "asn_names.json"


def _get_settings():
    """Get fresh settings (no caching here to allow test overrides)."""
    from app.settings import get_settings
    return get_settings()


# Create module-level getters that fetch settings dynamically
def _make_setting_getter(attr_name: str):
    """Create a getter function for a settings attribute."""
    def getter():
        return getattr(_get_settings(), attr_name)
    return getter


# Use properties with __getattr__ fallback for dynamic attribute access
def __getattr__(name: str):
    """Dynamically resolve settings attributes when accessed."""
    attr_map = {
        'CACHE_EXPIRE': 'cache_expire',
        'MASSCAN_RATE': 'masscan_rate',
        'SOCKET_TIMEOUT': 'socket_timeout',
        'REDIS_HOST': 'redis_host',
        'REDIS_PORT': 'redis_port',
        'REDIS_DB': 'redis_db',
        'REDIS_NAMESPACE': 'redis_namespace',
        'REDIS_PASSWORD': 'redis_password',
        'DEFAULT_PAGE_SIZE': 'default_page_size',
        'MAX_PAGE_SIZE': 'max_page_size',
        'SMTP_HOST': 'smtp_host',
        'SMTP_PORT': 'smtp_port',
        'SMTP_USER': 'smtp_user',
        'SMTP_PASSWORD': 'smtp_password',
        'SMTP_STARTTLS': 'smtp_starttls',
        'CONTACT_TO': 'contact_to',
        'CONTACT_FROM': 'contact_from',
        'CONTACT_RATE_LIMIT': 'contact_rate_limit',
        'CONTACT_RATE_WINDOW': 'contact_rate_window',
        'CONTACT_TOKEN': 'contact_token',
        'HCAPTCHA_SECRET': 'hcaptcha_secret',
        'RECAPTCHA_SECRET': 'recaptcha_secret',
        'CONTACT_IP_DENYLIST': 'contact_ip_denylist',
    }
    
    if name in attr_map:
        settings = _get_settings()
        return getattr(settings, attr_map[name])
    
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

