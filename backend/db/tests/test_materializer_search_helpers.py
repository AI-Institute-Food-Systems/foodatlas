"""Tests for materializer_search helper functions."""

from src.etl.materializer_search import (
    _build_exact_tokens,
    _extract_external_id_values,
)


class TestExtractExternalIdValues:
    def test_food_foodon_splits_url(self):
        ext_ids = {"foodon": ["http://purl.obolibrary.org/obo/FOODON_123"]}
        result = _extract_external_id_values(ext_ids, "food")
        assert result == ["FOODON_123"]

    def test_chemical_returns_raw_ids(self):
        ext_ids = {"pubchem_compound": ["12345", "67890"]}
        result = _extract_external_id_values(ext_ids, "chemical")
        assert result == ["12345", "67890"]

    def test_empty_ids(self):
        assert _extract_external_id_values({}, "food") == []

    def test_skips_non_list_values(self):
        ext_ids = {"bad": "not_a_list"}
        assert _extract_external_id_values(ext_ids, "food") == []


class TestBuildExactTokens:
    def test_includes_all_fields(self):
        # Tokens are lowercased to match the search repository's lowercased
        # query term.
        tokens = _build_exact_tokens(
            "food",
            "tomato",
            "Solanum lycopersicum",
            ["tomate"],
            ["FDC:123"],
        )
        assert "food" in tokens
        assert "tomato" in tokens
        assert "solanum lycopersicum" in tokens
        assert "tomate" in tokens
        assert "fdc:123" in tokens

    def test_skips_empty_scientific_name(self):
        tokens = _build_exact_tokens("chemical", "caffeine", "", [], [])
        assert "" not in tokens
        assert "chemical" in tokens
        assert "caffeine" in tokens

    def test_returns_strings(self):
        tokens = _build_exact_tokens("food", "a", "b", [], [])
        assert all(isinstance(t, str) for t in tokens)
