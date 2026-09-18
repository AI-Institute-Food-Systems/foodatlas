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
    # Public /v1/ API key store: AWS Secrets Manager secret holding a JSON
    # object {sha256_hex(key): {email, created, notes}}. Empty disables the
    # public key path (only the internal `key` is accepted on /v1/*).
    public_keys_secret_name: str = ""
    public_keys_refresh_seconds: int = 300
    aws_region: str = "us-west-1"
    # /v1/ per-API-key rate limit. The internal key (frontend SSR path)
    # always bypasses. Disabled in debug mode and when
    # ``rate_limit_enabled`` is False.
    rate_limit_enabled: bool = True
    rate_limit_per_minute: int = 60
    rate_limit_burst: int = 10
    # Structured per-request access log (see src/access_log.py). Scoped to the
    # public API by default: the internal UI routes are far higher volume and
    # are not what we attribute usage for. Off in debug, like the rate limiter.
    access_log_enabled: bool = True
    access_log_path_prefix: str = "/v1"
    # Mirror of the access log into the FoodAtlas umami website (see
    # src/umami_sink.py): one ``api_request`` event per external /v1 call.
    # Empty website id disables it. Deliberately NOT gated on ``debug`` so a
    # local run against a fake receiver can verify the payload.
    umami_website_id: str = ""
    umami_host_url: str = "https://umami.aifs.ucdavis.edu"
    umami_hostname: str = "api.foodatlas.ai"
    umami_sample_rate: float = 1.0
    umami_queue_size: int = 1000
