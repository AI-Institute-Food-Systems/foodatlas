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


@pytest.fixture()
def client(mock_db: AsyncMock) -> Generator[TestClient]:
    """FastAPI TestClient with mocked dependencies."""
    settings = _make_debug_settings()
    app = create_app(settings)

    async def _override_db() -> AsyncGenerator[AsyncMock]:
        yield mock_db

    async def _override_verify() -> None:
        return

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[verify_api_key] = _override_verify
    app.dependency_overrides[get_settings] = lambda: settings

    with TestClient(app, raise_server_exceptions=False) as tc:
        yield tc
