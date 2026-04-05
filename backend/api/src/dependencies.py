"""FastAPI dependencies: DB session and auth."""

from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

from fastapi import Depends, HTTPException, Request
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.config import APISettings


class DBSettings(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 5432
    name: str = "foodatlas"
    user: str = "foodatlas"
    password: str = "foodatlas"

    @property
    def async_url(self) -> str:
        return (
            f"postgresql+psycopg_async://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


@dataclass
class _State:
    """Module-level mutable state container."""

    session_factory: async_sessionmaker[AsyncSession] | None = field(default=None)
    settings: APISettings | None = field(default=None)


_state = _State()


def get_settings() -> APISettings:
    if _state.settings is None:
        _state.settings = APISettings()
    return _state.settings


def init_session_factory() -> None:
    """Initialize the async session factory (called at app startup)."""
    db = DBSettings()
    engine = create_async_engine(db.async_url, echo=False)
    _state.session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )


async def get_db(request: Request) -> AsyncGenerator[AsyncSession]:
    """Yield an async DB session."""
    if _state.session_factory is None:
        init_session_factory()
    factory = _state.session_factory
    assert factory is not None
    async with factory() as session:
        yield session


_settings_dep = Depends(get_settings)


async def verify_api_key(
    request: Request,
    settings: APISettings = _settings_dep,
) -> None:
    """Verify API key if configured. Skip in debug mode."""
    if settings.debug or not settings.key:
        return
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {settings.key}":
        raise HTTPException(status_code=401, detail="Invalid API key")
