"""Tests for src.etl.materializer_search — search helper functions."""

import pandas as pd
from src.etl.materializer_search import (
    _build_exact_tokens,
    _count_scoped_r2,
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


class TestCountScopedR2:
    """Test _count_scoped_r2 — IS_A edge counting from seeds to root."""

    @staticmethod
    def _make_r2(edges: list[tuple[str, str]]) -> pd.DataFrame:
        return pd.DataFrame(edges, columns=["head_id", "tail_id"])

    def test_basic_chain(self):
        # A -> B -> C, seed={A}
        r2 = self._make_r2([("A", "B"), ("B", "C")])
        assert _count_scoped_r2(r2, {"A"}, {"A", "B", "C"}) == 2

    def test_only_reachable_edges(self):
        # A -> B -> C, D -> E (disconnected), seed={A}
        r2 = self._make_r2([("A", "B"), ("B", "C"), ("D", "E")])
        assert _count_scoped_r2(r2, {"A"}, {"A", "B", "C", "D", "E"}) == 2

    def test_empty_seeds(self):
        r2 = self._make_r2([("A", "B")])
        assert _count_scoped_r2(r2, set(), {"A", "B"}) == 0

    def test_cycle_does_not_loop(self):
        # A -> B -> A (cycle)
        r2 = self._make_r2([("A", "B"), ("B", "A")])
        assert _count_scoped_r2(r2, {"A"}, {"A", "B"}) == 2

    def test_filters_by_type_ids(self):
        # A -> B -> C, but C not in type_ids
        r2 = self._make_r2([("A", "B"), ("B", "C")])
        assert _count_scoped_r2(r2, {"A"}, {"A", "B"}) == 1

    def test_diamond(self):
        # A -> B, A -> C, B -> D, C -> D
        r2 = self._make_r2([("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")])
        assert _count_scoped_r2(r2, {"A"}, {"A", "B", "C", "D"}) == 4

    def test_multiple_seeds(self):
        # A -> C, B -> C (seeds={A, B})
        r2 = self._make_r2([("A", "C"), ("B", "C")])
        assert _count_scoped_r2(r2, {"A", "B"}, {"A", "B", "C"}) == 2
