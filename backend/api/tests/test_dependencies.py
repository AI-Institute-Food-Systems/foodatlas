"""Tests for FastAPI dependencies: DB session factory and API key verification."""

import os
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from src.config import APISettings
from src.dependencies import DBSettings, _state, get_settings, verify_api_key


class TestDBSettings:
    """Verify DBSettings defaults and async_url property."""

    def test_defaults(self) -> None:
        settings = DBSettings(
            **{"_env_file": None},
        )
        assert settings.host == "localhost"
        assert settings.port == 5432
        assert settings.name == "foodatlas"
        assert settings.user == "foodatlas"
        assert settings.password == "foodatlas"

    def test_async_url(self) -> None:
        settings = DBSettings(
            **{"_env_file": None},
        )
        expected = (
            "postgresql+psycopg_async://foodatlas:foodatlas@localhost:5432/foodatlas"
        )
        assert settings.async_url == expected

    def test_async_url_custom(self) -> None:
        os.environ["DB_HOST"] = "db.example.com"
        os.environ["DB_PORT"] = "5433"
        os.environ["DB_NAME"] = "mydb"
        os.environ["DB_USER"] = "admin"
        os.environ["DB_PASSWORD"] = "s3cret"
        try:
            settings = DBSettings(
                **{"_env_file": None},
            )
            assert "db.example.com:5433/mydb" in settings.async_url
            assert "admin:s3cret" in settings.async_url
        finally:
            for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"):
                os.environ.pop(key, None)


class TestGetSettings:
    """Verify get_settings caches and returns APISettings."""

    def test_returns_api_settings(self) -> None:
        original = _state.settings
        try:
            _state.settings = None
            result = get_settings()
            assert isinstance(result, APISettings)
        finally:
            _state.settings = original


class TestVerifyApiKey:
    """Verify API key checking logic."""

    @pytest.mark.asyncio
    async def test_skip_in_debug_mode(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        settings.debug = True
        settings.key = "some-key"
        request = MagicMock()
        # Should not raise
        await verify_api_key(request, settings)

    @pytest.mark.asyncio
    async def test_skip_when_no_key_configured(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        settings.debug = False
        settings.key = ""
        request = MagicMock()
        # Should not raise
        await verify_api_key(request, settings)

    @pytest.mark.asyncio
    async def test_valid_bearer_token(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        settings.debug = False
        settings.key = "correct-key"
        request = MagicMock()
        request.headers.get.return_value = "Bearer correct-key"
        # Should not raise
        await verify_api_key(request, settings)

    @pytest.mark.asyncio
    async def test_missing_auth_header(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        settings.debug = False
        settings.key = "correct-key"
        request = MagicMock()
        request.headers.get.return_value = ""
        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key(request, settings)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_wrong_bearer_token(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        settings.debug = False
        settings.key = "correct-key"
        request = MagicMock()
        request.headers.get.return_value = "Bearer wrong-key"
        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key(request, settings)
        assert exc_info.value.status_code == 401
        assert exc_info.value.detail == "Invalid API key"
