"""Tests for src.config — DBSettings from environment variables."""

from unittest.mock import patch

from src.config import DBSettings


class TestDBSettingsDefaults:
    """Verify default values when no env vars are set."""

    def test_default_host(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.host == "localhost"

    def test_default_port(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.port == 5432

    def test_default_name(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.name == "foodatlas"

    def test_default_user(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.user == "foodatlas"

    def test_default_password(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.password == "foodatlas"


class TestDBSettingsFromEnv:
    """Verify env var overrides with DB_ prefix."""

    def test_host_from_env(self):
        with patch.dict("os.environ", {"DB_HOST": "db.prod.example.com"}):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.host == "db.prod.example.com"

    def test_port_from_env(self):
        with patch.dict("os.environ", {"DB_PORT": "5433"}):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.port == 5433

    def test_name_from_env(self):
        with patch.dict("os.environ", {"DB_NAME": "my_db"}):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.name == "my_db"

    def test_user_from_env(self):
        with patch.dict("os.environ", {"DB_USER": "admin"}):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.user == "admin"

    def test_password_from_env(self):
        with patch.dict("os.environ", {"DB_PASSWORD": "s3cret"}):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.password == "s3cret"


class TestDBSettingsURLs:
    """Verify sync_url and async_url property generation."""

    def test_sync_url_format(self):
        with patch.dict(
            "os.environ",
            {
                "DB_HOST": "myhost",
                "DB_PORT": "5433",
                "DB_NAME": "mydb",
                "DB_USER": "myuser",
                "DB_PASSWORD": "mypass",
            },
        ):
            settings = DBSettings(
                **{"_env_file": None},
            )
        expected = "postgresql+psycopg://myuser:mypass@myhost:5433/mydb"
        assert settings.sync_url == expected

    def test_async_url_format(self):
        with patch.dict(
            "os.environ",
            {
                "DB_HOST": "myhost",
                "DB_PORT": "5433",
                "DB_NAME": "mydb",
                "DB_USER": "myuser",
                "DB_PASSWORD": "mypass",
            },
        ):
            settings = DBSettings(
                **{"_env_file": None},
            )
        expected = "postgresql+psycopg_async://myuser:mypass@myhost:5433/mydb"
        assert settings.async_url == expected

    def test_sync_url_with_defaults(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert "postgresql+psycopg://" in settings.sync_url
        assert "localhost:5432/foodatlas" in settings.sync_url

    def test_async_url_with_defaults(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert "postgresql+psycopg_async://" in settings.async_url
        assert "localhost:5432/foodatlas" in settings.async_url


class TestDBSettingsExtra:
    """Verify extra env vars are ignored (extra='ignore')."""

    def test_extra_env_vars_ignored(self):
        with patch.dict(
            "os.environ",
            {
                "DB_HOST": "localhost",
                "DB_UNKNOWN_KEY": "value",
            },
        ):
            settings = DBSettings(
                **{"_env_file": None},
            )
        assert settings.host == "localhost"
        assert not hasattr(settings, "unknown_key")
