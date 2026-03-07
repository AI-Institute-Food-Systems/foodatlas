"""Tests for KGCSettings loading from env vars and defaults.json."""

import os

from src.models.settings import KGCSettings


class TestKGCSettings:
    def test_defaults_from_json(self):
        settings = KGCSettings()
        assert settings.kg_dir == "data/kg"
        assert settings.output_dir == "data/output"
        assert settings.cache_dir == "data/cache"
        assert settings.output_format == "jsonl"

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("KGC_KG_DIR", "/custom/kg")
        monkeypatch.setenv("KGC_OUTPUT_FORMAT", "parquet")
        settings = KGCSettings()
        assert settings.kg_dir == "/custom/kg"
        assert settings.output_format == "parquet"

    def test_env_var_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("KGC_OUTPUT_DIR", "/env/output")
        settings = KGCSettings()
        assert settings.output_dir == "/env/output"
        # defaults.json fallback still works for others
        assert settings.cache_dir == "data/cache"

    def test_api_key_defaults_empty(self):
        # Ensure no env vars leak in
        env = os.environ.copy()
        for key in ("KGC_OPENAI_API_KEY", "KGC_NCBI_API_KEY"):
            env.pop(key, None)
        settings = KGCSettings()
        # defaults.json has empty strings
        assert settings.openai_api_key == ""
        assert settings.ncbi_api_key == ""
