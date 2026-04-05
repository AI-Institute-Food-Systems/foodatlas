"""SQLAlchemy engine and session factories."""

from collections.abc import AsyncGenerator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from .config import DBSettings


def create_sync_engine(settings: DBSettings | None = None):
    """Create a synchronous engine for ETL operations."""
    settings = settings or DBSettings()
    return create_engine(settings.sync_url, echo=False)


def create_async_eng(settings: DBSettings | None = None):
    """Create an async engine for the API layer."""
    settings = settings or DBSettings()
    return create_async_engine(settings.async_url, echo=False)


def get_sync_session_factory(settings: DBSettings | None = None):
    """Create a sync session factory."""
    engine = create_sync_engine(settings)
    return sessionmaker(engine, class_=Session, expire_on_commit=False)


def get_async_session_factory(settings: DBSettings | None = None):
    """Create an async session factory."""
    engine = create_async_eng(settings)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_async_session(
    settings: DBSettings | None = None,
) -> AsyncGenerator[AsyncSession]:
    """Yield an async session for FastAPI dependency injection."""
    factory = get_async_session_factory(settings)
    async with factory() as session:
        yield session
