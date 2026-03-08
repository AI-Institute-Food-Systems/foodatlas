"""Tests for KGCSettings loading from env vars and defaults.json."""

from src.models.settings import KGCSettings


class TestKGCSettings:
    def test_defaults_from_json(self):
        settings = KGCSettings()
        assert settings.kg_dir == "data/kg"
        assert settings.output_dir == "data/output"
        assert settings.cache_dir == "outputs/cache"

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("KGC_KG_DIR", "/custom/kg")
        settings = KGCSettings()
        assert settings.kg_dir == "/custom/kg"

    def test_env_var_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("KGC_OUTPUT_DIR", "/env/output")
        settings = KGCSettings()
        assert settings.output_dir == "/env/output"
        # defaults.json fallback still works for others
        assert settings.cache_dir == "outputs/cache"

    def test_data_cleaning_dir_from_pipeline(self):
        settings = KGCSettings()
        assert settings.data_cleaning_dir == "outputs/data_cleaning"

    def test_data_cleaning_dir_override(self):
        settings = KGCSettings(
            pipeline={"stages": {"data_cleaning": {"output_dir": "/custom"}}}
        )
        assert settings.data_cleaning_dir == "/custom"

    def test_api_key_defaults_empty(self, monkeypatch):
        monkeypatch.delenv("KGC_OPENAI_API_KEY", raising=False)
        settings = KGCSettings()
        assert settings.openai_api_key == ""

    def test_ncbi_from_dotenv(self, monkeypatch):
        monkeypatch.setenv("NCBI_EMAIL", "test@example.com")
        monkeypatch.setenv("NCBI_API_KEY", "abc123")
        settings = KGCSettings()
        assert settings.ncbi_email == "test@example.com"
        assert settings.ncbi_api_key == "abc123"
