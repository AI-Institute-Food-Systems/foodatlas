"""FastAPI dependencies: DB session and auth."""

import secrets
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
from src.public_keys import get_store


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


def _matches(candidate: str, expected: str) -> bool:
    """Compare a caller-supplied token against a secret in constant time.

    A plain ``==`` on ``str`` short-circuits at the first differing byte, so the
    time it takes to reject leaks how long a prefix the caller got right — enough
    to recover the key one character at a time. Both sides are encoded first:
    :func:`secrets.compare_digest` raises on non-ASCII ``str``, and the candidate
    comes straight off the wire.
    """
    return secrets.compare_digest(candidate.encode("utf-8"), expected.encode("utf-8"))


async def verify_api_key(
    request: Request,
    settings: APISettings = _settings_dep,
) -> None:
    """Verify API key if configured. Skip in debug mode."""
    if settings.debug or not settings.key:
        return
    auth = request.headers.get("Authorization", "")
    if not _matches(auth, f"Bearer {settings.key}"):
        raise HTTPException(status_code=401, detail="Invalid API key")


async def verify_v1_key(
    request: Request,
    settings: APISettings = _settings_dep,
) -> None:
    """Authorise /v1/* requests with either the internal key or a public key.

    Accept order: debug bypass → internal ``settings.key`` (the frontend) →
    sha256 hash matches a record in :class:`PublicKeyStore`. Misses 401.

    The matched public key's email, non-secret prefix and org are stashed on
    ``request.state`` so :mod:`src.access_log` (and the umami sink it feeds)
    can attribute the request without re-reading (or ever logging) the
    Authorization header.
    """
    if settings.debug:
        return
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid API key")
    token = auth[len("Bearer ") :]
    if settings.key and _matches(token, settings.key):
        request.state.api_key_email = "internal"
        request.state.api_key_prefix = ""
        request.state.api_key_org = "internal"
        return
    store = get_store()
    record = store.verify(token) if store is not None else None
    if record is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    request.state.api_key_email = record.email
    request.state.api_key_prefix = record.prefix
    request.state.api_key_org = record.org
