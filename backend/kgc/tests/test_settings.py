"""Tests for KGCSettings loading from env vars and defaults.json."""

from src.models.settings import KGCSettings


class TestKGCSettings:
    def test_defaults_from_json(self):
        settings = KGCSettings()
        assert settings.kg_dir == "outputs/kg"
        assert settings.output_dir == "outputs"
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

    def test_ie_raw_dir_default(self):
        settings = KGCSettings()
        assert settings.ie_raw_dir == "../ie/outputs/extraction"
