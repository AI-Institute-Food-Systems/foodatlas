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
    # Per-attestation LLM-plausibility score at-or-below this is "low trust"
    # (the comparison is inclusive — score <= threshold counts as low).
    # The composition / data-points endpoints filter on this when the
    # `trust` query param is "default" (hide low-trust) or "low_only"
    # (show only low-trust). Threshold lives here so it can be tuned
    # without redeploying the frontend or rerunning the trust stage.
    trust_low_threshold: float = 0.4
