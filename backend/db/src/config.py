"""Database configuration via environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class DBSettings(BaseSettings):
    """PostgreSQL connection settings, read from DB_* env vars."""

    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 5432
    name: str = "foodatlas"
    user: str = "foodatlas"
    password: str = "foodatlas"

    @property
    def sync_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def async_url(self) -> str:
        return (
            f"postgresql+psycopg_async://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )
