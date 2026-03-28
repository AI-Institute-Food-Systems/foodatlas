"""Tests for discovery/query.py and discovery/cache.py."""

import importlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import src.discovery.query as query_module
from src.discovery.cache import (
    incremental_save,
    load_cached,
    save_cached,
)
from src.discovery.query import (
    query_ncbi_taxonomy,
    query_pubchem_compound,
)
from src.models.settings import KGCSettings


class TestCacheLoadSave:
    def test_load_missing_file_returns_empty(self, tmp_path: Path):
        result = load_cached(tmp_path, "nonexistent.json")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_round_trip(self, tmp_path: Path):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        save_cached(df, tmp_path, "test.json")
        loaded = load_cached(tmp_path, "test.json")
        assert list(loaded.columns) == ["a", "b"]
        assert len(loaded) == 2

    def test_save_creates_parent_dirs(self, tmp_path: Path):
        nested = tmp_path / "sub" / "dir"
        df = pd.DataFrame({"x": [1]})
        save_cached(df, nested, "test.json")
        assert (nested / "test.json").exists()

    def test_cache_hit(self, tmp_path: Path):
        df = pd.DataFrame({"col": [42]})
        save_cached(df, tmp_path, "cached.json")
        loaded = load_cached(tmp_path, "cached.json")
        assert loaded["col"].iloc[0] == 42

    def test_cache_miss(self, tmp_path: Path):
        loaded = load_cached(tmp_path, "missing.json")
        assert loaded.empty

    def test_preserves_nested_types(self, tmp_path: Path):
        df = pd.DataFrame({"ids": [["a", "b"]], "meta": [{"k": 1}]})
        save_cached(df, tmp_path, "nested.json")
        loaded = load_cached(tmp_path, "nested.json")
        assert loaded["ids"].iloc[0] == ["a", "b"]
        assert loaded["meta"].iloc[0] == {"k": 1}


class TestIncrementalSave:
    def test_appends_and_clears_batch(self, tmp_path: Path):
        acc = pd.DataFrame(columns=["Count", "search_term"])
        rows = [{"Count": 1}, {"Count": 2}]
        names = ["apple", "banana"]
        updated, cleared = incremental_save(
            acc, rows, names, "search_term", tmp_path, "inc.json"
        )
        assert len(updated) == 2
        assert cleared == []
        assert (tmp_path / "inc.json").exists()
        assert list(updated["search_term"]) == ["apple", "banana"]


class TestQuerySignatures:
    def test_ncbi_accepts_settings(self, tmp_path: Path):
        settings = KGCSettings(
            cache_dir=str(tmp_path),
            ncbi_email="test@example.com",
            ncbi_api_key="fake_key",
        )
        mock_entrez = MagicMock()
        mock_entrez.esearch.return_value = MagicMock()
        mock_entrez.read.return_value = {
            "Count": "0",
            "RetMax": "0",
            "RetStart": "0",
            "IdList": [],
            "TranslationSet": [],
            "TranslationStack": [],
            "QueryTranslation": "",
            "WarningList": None,
        }
        with (
            patch("src.discovery.query.Entrez", mock_entrez),
            patch("src.discovery.query._entrez", mock_entrez),
        ):
            result = query_ncbi_taxonomy(
                ["apple"],
                path_kg=None,
                path_cache_dir=None,
                settings=settings,
            )
            assert isinstance(result, pd.DataFrame)
            assert mock_entrez.email == "test@example.com"
            assert mock_entrez.api_key == "fake_key"

    def test_pubchem_accepts_settings(self, tmp_path: Path):
        settings = KGCSettings(
            cache_dir=str(tmp_path),
            ncbi_email="test@example.com",
        )
        result = query_pubchem_compound(
            ["caffeine"],
            path_kg=None,
            path_cache_dir=None,
            settings=settings,
        )
        assert isinstance(result, pd.DataFrame)


class TestQueryErrorHandling:
    def test_ncbi_no_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir must be provided"):
            query_ncbi_taxonomy(["apple"], path_kg=None, path_cache_dir=None)

    def test_pubchem_no_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir must be provided"):
            query_pubchem_compound(["caffeine"], path_kg=None, path_cache_dir=None)

    def test_pubchem_new_names_no_mapping_logs_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ):
        settings = KGCSettings(
            cache_dir=str(tmp_path),
            ncbi_email="test@example.com",
            pubchem_mapping_file="",
        )
        with caplog.at_level("ERROR"):
            query_pubchem_compound(
                ["novelchemical123"],
                path_kg=None,
                path_cache_dir=None,
                settings=settings,
            )
        assert "new chemical names" in caplog.text

    def test_pubchem_mapping_file_used(self, tmp_path: Path):
        mapping_file = tmp_path / "mapping.txt"
        mapping_file.write_text("caffeine\t2519\n")

        settings = KGCSettings(
            cache_dir=str(tmp_path / "cache"),
            ncbi_email="test@example.com",
            pubchem_mapping_file=str(mapping_file),
        )

        with patch("src.discovery.query.Entrez") as mock_entrez:
            mock_entrez.esummary.return_value = MagicMock()
            mock_entrez.read.return_value = [
                {"CID": 2519, "IUPACName": "caffeine", "SynonymList": ["caffeine"]},
            ]
            result = query_pubchem_compound(
                ["caffeine"],
                path_kg=None,
                path_cache_dir=None,
                settings=settings,
            )
            assert isinstance(result, pd.DataFrame)


class TestNoModuleSideEffects:
    def test_import_does_not_set_entrez(self):
        """Importing the module should not set Entrez at module level."""
        importlib.reload(query_module)
        assert not hasattr(query_module, "_entrez_configured")
