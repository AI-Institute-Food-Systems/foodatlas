"""Tests for APISettings configuration."""

import os

from src.config import APISettings


class TestAPISettingsDefaults:
    """Verify default values when no env vars are set."""

    def test_default_key_is_empty(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        assert settings.key == ""

    def test_default_cors_origins(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        assert settings.cors_origins == "http://localhost:3000"

    def test_default_debug_is_true(self) -> None:
        settings = APISettings(
            **{"_env_file": None},
        )
        assert settings.debug is True


class TestAPISettingsFromEnv:
    """Verify env var overrides via API_ prefix."""

    def test_key_from_env(self, monkeypatch: object) -> None:
        os.environ["API_KEY"] = "test-secret-123"
        try:
            settings = APISettings(
                **{"_env_file": None},
            )
            assert settings.key == "test-secret-123"
        finally:
            os.environ.pop("API_KEY", None)

    def test_cors_origins_from_env(self, monkeypatch: object) -> None:
        os.environ["API_CORS_ORIGINS"] = "https://example.com,https://other.com"
        try:
            settings = APISettings(
                **{"_env_file": None},
            )
            assert settings.cors_origins == "https://example.com,https://other.com"
        finally:
            os.environ.pop("API_CORS_ORIGINS", None)

    def test_debug_from_env(self) -> None:
        os.environ["API_DEBUG"] = "false"
        try:
            settings = APISettings(
                **{"_env_file": None},
            )
            assert settings.debug is False
        finally:
            os.environ.pop("API_DEBUG", None)

    def test_extra_env_vars_ignored(self) -> None:
        os.environ["API_UNKNOWN_FIELD"] = "whatever"
        try:
            settings = APISettings(
                **{"_env_file": None},
            )
            assert not hasattr(settings, "unknown_field")
        finally:
            os.environ.pop("API_UNKNOWN_FIELD", None)
