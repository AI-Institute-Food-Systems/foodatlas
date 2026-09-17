"""Shared fixtures for API tests."""

from collections.abc import AsyncGenerator, Generator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from src.app import create_app
from src.config import APISettings
from src.dependencies import get_db, get_settings, verify_api_key, verify_v1_key
from testcontainers.postgres import PostgresContainer

from tests import dbharness


def _make_debug_settings() -> APISettings:
    settings = APISettings(**{"_env_file": None})
    settings.debug = True
    return settings


@pytest.fixture()
def mock_db() -> AsyncMock:
    """Provide a mock async DB session."""
    return AsyncMock()


def _build_client(settings: APISettings, mock_db: AsyncMock) -> TestClient:
    app = create_app(settings)

    async def _override_db() -> AsyncGenerator[AsyncMock]:
        yield mock_db

    async def _override_verify() -> None:
        return

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[verify_api_key] = _override_verify
    app.dependency_overrides[verify_v1_key] = _override_verify
    app.dependency_overrides[get_settings] = lambda: settings

    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def client(mock_db: AsyncMock) -> Generator[TestClient]:
    """FastAPI TestClient with mocked dependencies."""
    with _build_client(_make_debug_settings(), mock_db) as tc:
        yield tc


@pytest.fixture()
def client_with_downloads_bucket(mock_db: AsyncMock) -> Generator[TestClient]:
    """TestClient with a configured downloads bucket for /download tests."""
    settings = _make_debug_settings()
    settings.downloads_bucket = "test-downloads-bucket"
    settings.downloads_region = "us-west-1"
    with _build_client(settings, mock_db) as tc:
        yield tc


# --- Real-Postgres fixtures -------------------------------------------
#
# Only `test_filter_predicates_pg.py` uses these. They are here rather
# than in that module so a second DB-backed suite doesn't spin up a
# second container. See tests/dbharness.py for why SQLite isn't enough.


@pytest.fixture(scope="session")
def pg_url() -> Generator[str]:
    """A throwaway Postgres for the session; skips (or fails) without Docker."""
    reason = dbharness.docker_unavailable()
    if reason:
        if dbharness.require_db_tests():
            pytest.fail(
                f"REQUIRE_DB_TESTS is set but Postgres cannot start: {reason}. "
                "These are the only tests that execute filter SQL; refusing "
                "to pass by skipping them."
            )
        pytest.skip(f"Postgres-backed tests need Docker ({reason})")

    with PostgresContainer("postgres:16-alpine", driver="psycopg") as pg:
        yield pg.get_connection_url()


@pytest_asyncio.fixture()
async def pg_session(pg_url: str) -> AsyncGenerator[AsyncSession]:
    """Session against a freshly-created schema, torn down per test.

    Per-test DDL rather than per-session: these tests insert conflicting
    fixtures on purpose (a row set that matches one filter and not
    another), and leaking rows between them would make failures depend on
    execution order.
    """
    engine = create_async_engine(pg_url.replace("+psycopg2", "+psycopg"), echo=False)
    try:
        async with engine.begin() as conn:
            for ddl in dbharness.DDL:
                await conn.execute(text(ddl))
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            yield session
        async with engine.begin() as conn:
            for ddl in dbharness.DDL:
                tbl = ddl.split("CREATE TABLE ")[1].split(" ")[0].strip()
                await conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
    finally:
        await engine.dispose()
