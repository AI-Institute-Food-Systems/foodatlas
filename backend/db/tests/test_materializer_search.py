"""Tests for src.etl.materializer_search — search helper functions."""

from src.etl.materializer_search import (
    _build_exact_tokens,
    _extract_external_id_values,
)


class TestExtractExternalIdValues:
    """Test _extract_external_id_values."""

    def test_basic_extraction(self):
        ext_ids = {"chebi": ["CHEBI:29073", "CHEBI:12345"]}
        result = _extract_external_id_values(ext_ids, "chemical")
        assert result == ["CHEBI:29073", "CHEBI:12345"]

    def test_food_foodon_extracts_last_segment(self):
        ext_ids = {"foodon": ["http://example.org/foodon/FOO_123"]}
        result = _extract_external_id_values(ext_ids, "food")
        assert result == ["FOO_123"]

    def test_non_food_foodon_kept_as_is(self):
        ext_ids = {"foodon": ["http://example.org/foodon/FOO_123"]}
        result = _extract_external_id_values(ext_ids, "chemical")
        assert result == ["http://example.org/foodon/FOO_123"]

    def test_multiple_keys(self):
        ext_ids = {
            "chebi": ["CHEBI:1"],
            "cdno": ["CDNO:2"],
        }
        result = _extract_external_id_values(ext_ids, "chemical")
        assert "CHEBI:1" in result
        assert "CDNO:2" in result

    def test_non_list_value_skipped(self):
        ext_ids = {"chebi": "not_a_list"}
        result = _extract_external_id_values(ext_ids, "chemical")
        assert result == []

    def test_empty_dict(self):
        result = _extract_external_id_values({}, "food")
        assert result == []


class TestBuildExactTokens:
    """Test _build_exact_tokens."""

    def test_basic_tokens(self):
        result = _build_exact_tokens(
            "food",
            "Apple",
            "Malus domestica",
            ["apple fruit"],
            ["FOODON:123"],
        )
        assert result == [
            "food",
            "Apple",
            "Malus domestica",
            "apple fruit",
            "FOODON:123",
        ]

    def test_no_scientific_name(self):
        result = _build_exact_tokens("chemical", "Vitamin C", "", ["ascorbic acid"], [])
        assert "Vitamin C" in result
        assert "" not in result
        assert "ascorbic acid" in result

    def test_entity_type_always_first(self):
        result = _build_exact_tokens("disease", "Scurvy", "", [], [])
        assert result[0] == "disease"
        assert result[1] == "Scurvy"

    def test_empty_synonyms_and_ext_ids(self):
        result = _build_exact_tokens("food", "X", "", [], [])
        assert result == ["food", "X"]

    def test_all_values_are_strings(self):
        nums_as_synonyms: list[str] = ["123"]
        nums_as_ext_ids: list[str] = ["456"]
        result = _build_exact_tokens(
            "food",
            "Apple",
            "Malus",
            nums_as_synonyms,
            nums_as_ext_ids,
        )
        for token in result:
            assert isinstance(token, str)
