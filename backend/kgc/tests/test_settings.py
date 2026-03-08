"""Tests for KGCSettings loading from env vars and defaults.json."""

import os

from src.models.settings import KGCSettings


class TestKGCSettings:
    def test_defaults_from_json(self):
        settings = KGCSettings()
        assert settings.kg_dir == "data/kg"
        assert settings.output_dir == "data/output"
        assert settings.cache_dir == "data/cache"

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("KGC_KG_DIR", "/custom/kg")
        settings = KGCSettings()
        assert settings.kg_dir == "/custom/kg"

    def test_env_var_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("KGC_OUTPUT_DIR", "/env/output")
        settings = KGCSettings()
        assert settings.output_dir == "/env/output"
        # defaults.json fallback still works for others
        assert settings.cache_dir == "data/cache"

    def test_data_cleaning_dir_from_pipeline(self):
        settings = KGCSettings()
        assert settings.data_cleaning_dir == "outputs/data_cleaning"

    def test_data_cleaning_dir_override(self):
        settings = KGCSettings(
            pipeline={
                "stages": {"integration": {"data_cleaning": {"output_dir": "/custom"}}}
            }
        )
        assert settings.data_cleaning_dir == "/custom"

    def test_api_key_defaults_empty(self):
        # Ensure no env vars leak in
        env = os.environ.copy()
        for key in ("KGC_OPENAI_API_KEY", "KGC_NCBI_API_KEY"):
            env.pop(key, None)
        settings = KGCSettings()
        # defaults.json has empty strings
        assert settings.openai_api_key == ""
        assert settings.ncbi_api_key == ""
