"""API configuration."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):
    """API settings, read from API_* env vars."""

    model_config = SettingsConfigDict(
        env_prefix="API_", env_file=".env", extra="ignore"
    )

    key: str = ""
    cors_origins: str = "http://localhost:3000"
    debug: bool = True
    downloads_bucket: str = ""
    downloads_region: str = "us-west-1"
