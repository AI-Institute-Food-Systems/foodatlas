"""Shared fixtures for API tests."""

from collections.abc import AsyncGenerator, Generator
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from src.app import create_app
from src.config import APISettings
from src.dependencies import get_db, get_settings, verify_api_key


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
