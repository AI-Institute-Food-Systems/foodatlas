"""Tests for src.engine — engine and session factory creation."""

from unittest.mock import patch

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import sessionmaker
from src.config import DBSettings
from src.engine import (
    create_async_eng,
    create_sync_engine,
    get_async_session,
    get_async_session_factory,
    get_sync_session_factory,
)


def _test_settings() -> DBSettings:
    """Create a DBSettings with known values for testing."""
    with patch.dict(
        "os.environ",
        {
            "DB_HOST": "testhost",
            "DB_PORT": "5555",
            "DB_NAME": "testdb",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpass",
        },
    ):
        return DBSettings(
            **{"_env_file": None},
        )


class TestCreateSyncEngine:
    """Test create_sync_engine returns a valid SQLAlchemy Engine."""

    def test_returns_engine(self):
        settings = _test_settings()
        engine = create_sync_engine(settings)
        assert isinstance(engine, Engine)

    def test_engine_url_matches_settings(self):
        settings = _test_settings()
        engine = create_sync_engine(settings)
        url_str = str(engine.url)
        assert "psycopg" in url_str
        assert "testhost" in url_str
        assert "5555" in url_str
        assert "testdb" in url_str

    def test_creates_engine_without_explicit_settings(self):
        engine = create_sync_engine()
        assert isinstance(engine, Engine)


class TestCreateAsyncEngine:
    """Test create_async_eng returns a valid async engine."""

    def test_returns_async_engine(self):
        settings = _test_settings()
        engine = create_async_eng(settings)
        url_str = str(engine.url)
        assert "psycopg_async" in url_str

    def test_engine_url_matches_settings(self):
        settings = _test_settings()
        engine = create_async_eng(settings)
        url_str = str(engine.url)
        assert "testhost" in url_str
        assert "5555" in url_str

    def test_creates_engine_without_explicit_settings(self):
        engine = create_async_eng()
        url_str = str(engine.url)
        assert "psycopg_async" in url_str


class TestSyncSessionFactory:
    """Test get_sync_session_factory returns a sessionmaker."""

    def test_returns_sessionmaker(self):
        settings = _test_settings()
        factory = get_sync_session_factory(settings)
        assert isinstance(factory, sessionmaker)


class TestAsyncSessionFactory:
    """Test get_async_session_factory returns an async_sessionmaker."""

    def test_returns_async_sessionmaker(self):
        settings = _test_settings()
        factory = get_async_session_factory(settings)
        assert isinstance(factory, async_sessionmaker)


class TestGetAsyncSession:
    """Test get_async_session yields an AsyncSession."""

    def test_yields_async_session(self):
        """Verify the generator protocol (no real DB connection)."""
        settings = _test_settings()
        gen = get_async_session(settings)
        assert hasattr(gen, "__aiter__")
        assert hasattr(gen, "__anext__")
