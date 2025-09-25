from functools import lru_cache
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Core
    mongo_uri: str = Field(
        default="mongodb://localhost:27017",
        alias="MONGO_URI",
    )
    db_name: str = Field(default="ip_data", alias="DB_NAME")

    # Redis
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    redis_namespace: str = Field(default="purple_jo", alias="REDIS_NAMESPACE")
    redis_password: Optional[str] = Field(default=None, alias="REDIS_PASSWORD")

    # Pagination
    default_page_size: int = Field(default=25, alias="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(default=200, alias="MAX_PAGE_SIZE")

    # Contact / SMTP
    smtp_host: str = Field(default="localhost", alias="SMTP_HOST")
    smtp_port: int = Field(default=25, alias="SMTP_PORT")
    smtp_user: Optional[str] = Field(default=None, alias="SMTP_USER")
    smtp_password: Optional[str] = Field(default=None, alias="SMTP_PASSWORD")
    smtp_starttls: bool = Field(default=False, alias="SMTP_STARTTLS")

    contact_to: str = Field(default="hello@netscanner.io", alias="CONTACT_TO")
    contact_from: Optional[str] = Field(default=None, alias="CONTACT_FROM")
    contact_rate_limit: int = Field(default=5, alias="CONTACT_RATE_LIMIT")
    contact_rate_window: int = Field(default=3600, alias="CONTACT_RATE_WINDOW")
    contact_token: Optional[str] = Field(default=None, alias="CONTACT_TOKEN")

    # Captcha
    hcaptcha_secret: Optional[str] = Field(
        default=None,
        alias="HCAPTCHA_SECRET",
    )
    recaptcha_secret: Optional[str] = Field(
        default=None,
        alias="RECAPTCHA_SECRET",
    )

    # Security / Admin
    contact_ip_denylist: str = Field(default="", alias="CONTACT_IP_DENYLIST")

    # Misc
    cache_expire: int = Field(default=86400, alias="CACHE_EXPIRE")
    masscan_rate: int = Field(default=1000, alias="MASSCAN_RATE")
    socket_timeout: int = Field(default=3, alias="SOCKET_TIMEOUT")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    def recommended_warnings(self) -> List[str]:
        warnings: List[str] = []
        # Contact security suggestions
        if not (
            self.hcaptcha_secret
            or self.recaptcha_secret
            or self.contact_token
        ):
            warnings.append(
                "No CAPTCHA or CONTACT_TOKEN; contact form is unprotected."
            )
        if self.contact_rate_limit > 20:
            warnings.append(
                (
                    "CONTACT_RATE_LIMIT="
                    f"{self.contact_rate_limit} is high; "
                    "consider lowering."
                )
            )
        if self.smtp_host == "localhost":
            warnings.append(
                (
                    "SMTP_HOST=localhost; ensure a relay is running "
                    "or emails will fail."
                )
            )
        return warnings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[arg-type]


def reset_settings_cache() -> None:
    """Clear cached settings (intended for test usage)."""
    try:
        get_settings.cache_clear()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass
