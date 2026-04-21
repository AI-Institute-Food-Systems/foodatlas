"""Tests for materializer helper functions."""

import json
from unittest.mock import MagicMock

import pandas as pd
from src.etl.materializer import (
    _build_sibling_map,
    _collect_ancestors,
    _insert_mv_entities,
    _render_siblings_col,
)
from src.etl.materializer_composition import (
    _add_foodatlas_evidence,
    _compute_median,
)


class TestComputeMedian:
    """Test _compute_median with various inputs."""

    def test_single_value(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 10.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result is not None
        assert result["unit"] == "mg/100g"
        assert result["value"] == 10.0

    def test_multiple_values_median(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 2.0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 8.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result is not None
        assert result["value"] == 5.0

    def test_skips_zero_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 0.0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 6.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result is not None
        assert result["value"] == 6.0

    def test_skips_non_mg_units(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 5.0, "unit": "g/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result is None

    def test_skips_none_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": None, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result is None

    def test_empty_evidences(self):
        result = _compute_median([])
        assert result is None

    def test_no_extraction_key(self):
        evidences = [{"other": "data"}]
        result = _compute_median(evidences)
        assert result is None

    def test_decimal_precision(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 1.5, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 2.5, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 3.5, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result is not None
        assert result["value"] == 2.5


class TestAddFoodatlasEvidence:
    """Test _add_foodatlas_evidence grouping logic."""

    def test_creates_new_entry(self):
        evidences: list = []
        ref = {"pmcid": "PMC123", "text": "Study premise."}
        extraction = {"method": "lit2kg"}
        _add_foodatlas_evidence(evidences, ref, extraction)
        assert len(evidences) == 1
        assert evidences[0]["premise"] == "Study premise."
        assert evidences[0]["reference"]["id"] == "PMC123"
        assert evidences[0]["reference"]["source_name"] == "FoodAtlas"

    def test_groups_by_premise(self):
        evidences: list = []
        ref = {"pmcid": "PMC123", "text": "Same premise."}
        _add_foodatlas_evidence(evidences, ref, {"method": "a"})
        _add_foodatlas_evidence(evidences, ref, {"method": "b"})
        assert len(evidences) == 1
        assert len(evidences[0]["extraction"]) == 2

    def test_different_premises_create_separate_entries(self):
        evidences: list = []
        _add_foodatlas_evidence(evidences, {"pmcid": "PMC1", "text": "Premise A"}, {})
        _add_foodatlas_evidence(evidences, {"pmcid": "PMC2", "text": "Premise B"}, {})
        assert len(evidences) == 2

    def test_url_with_pmcid(self):
        evidences: list = []
        ref = {"pmcid": "PMC999", "text": "Test"}
        _add_foodatlas_evidence(evidences, ref, {})
        url = evidences[0]["reference"]["url"]
        assert "PMC999" in url

    def test_url_without_pmcid(self):
        evidences: list = []
        ref = {"pmcid": "", "text": "Test"}
        _add_foodatlas_evidence(evidences, ref, {})
        assert evidences[0]["reference"]["url"] == ""


class TestInsertMvEntities:
    """Test _insert_mv_entities delegates to bulk_copy."""

    def test_calls_bulk_copy_with_base_and_extra_cols(self):
        conn = MagicMock()
        df = pd.DataFrame()
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "src.etl.materializer.bulk_copy"
        ) as mock_copy:
            _insert_mv_entities(conn, "mv_test", df, ["extra_col"])
            mock_copy.assert_called_once()
            args = mock_copy.call_args[0]
            assert args[1] == "mv_test"
            assert "foodatlas_id" in args[3]
            assert "extra_col" in args[3]

    def test_no_extra_cols(self):
        conn = MagicMock()
        df = pd.DataFrame()
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "src.etl.materializer.bulk_copy"
        ) as mock_copy:
            _insert_mv_entities(conn, "mv_test", df, [])
            cols = mock_copy.call_args[0][3]
            assert len(cols) == 6
            assert "external_ids" in cols


class TestBuildSiblingMap:
    """Test _build_sibling_map from ambiguous attestation candidates."""

    def test_empty_returns_empty(self):
        att = pd.DataFrame({"head_candidates": [], "tail_candidates": []})
        assert _build_sibling_map(att) == {}

    def test_unambiguous_ignored(self):
        att = pd.DataFrame({"head_candidates": [["a"]], "tail_candidates": [["b"]]})
        assert _build_sibling_map(att) == {}

    def test_ambiguous_head_builds_pairwise(self):
        att = pd.DataFrame(
            {
                "head_candidates": [["a", "b", "c"]],
                "tail_candidates": [["t"]],
            }
        )
        s = _build_sibling_map(att)
        assert s["a"] == {"b", "c"}
        assert s["b"] == {"a", "c"}
        assert s["c"] == {"a", "b"}

    def test_ambiguous_tail_builds_pairwise(self):
        att = pd.DataFrame(
            {
                "head_candidates": [["h"]],
                "tail_candidates": [["x", "y"]],
            }
        )
        s = _build_sibling_map(att)
        assert s["x"] == {"y"}
        assert s["y"] == {"x"}

    def test_symmetric_via_connected_components(self):
        """b and c never co-occur directly but are linked via a -> symmetric."""
        att = pd.DataFrame(
            {
                "head_candidates": [["a", "b"], ["a", "c"]],
                "tail_candidates": [["t"], ["t"]],
            }
        )
        s = _build_sibling_map(att)
        # Every entity in the cluster sees every other
        assert s["a"] == {"b", "c"}
        assert s["b"] == {"a", "c"}
        assert s["c"] == {"a", "b"}

    def test_separate_components_stay_isolated(self):
        att = pd.DataFrame(
            {
                "head_candidates": [["a", "b"], ["c", "d"]],
                "tail_candidates": [["t"], ["t"]],
            }
        )
        s = _build_sibling_map(att)
        assert s["a"] == {"b"}
        assert s["c"] == {"d"}

    def test_head_and_tail_merged(self):
        """Chemical appears as r1 tail and r3r4 head -> unified cluster."""
        att = pd.DataFrame(
            {
                "head_candidates": [["food1"], ["chem1", "chem2"]],
                "tail_candidates": [["chem1", "chem2"], ["disease1"]],
            }
        )
        s = _build_sibling_map(att)
        # chem1 and chem2 linked via both positions
        assert s["chem1"] == {"chem2"}
        assert s["chem2"] == {"chem1"}


class TestRenderSiblingsCol:
    """Test _render_siblings_col JSON serialization."""

    def test_empty_siblings_yields_empty_list(self):
        ids = pd.Series(["e1"])
        result = _render_siblings_col(ids, {}, {"e1": "apple"})
        assert result == ["[]"]

    def test_siblings_mapped_to_names(self):
        ids = pd.Series(["e1"])
        sibling_map = {"e1": {"e2", "e3"}}
        name_map = {"e1": "apple", "e2": "apricot", "e3": "pear"}
        result = json.loads(_render_siblings_col(ids, sibling_map, name_map)[0])
        assert {r["foodatlas_id"] for r in result} == {"e2", "e3"}
        assert {r["common_name"] for r in result} == {"apricot", "pear"}

    def test_unknown_id_falls_back_to_id(self):
        ids = pd.Series(["e1"])
        sibling_map = {"e1": {"e_missing"}}
        result = json.loads(_render_siblings_col(ids, sibling_map, {})[0])
        assert result[0]["common_name"] == "e_missing"


class TestCollectAncestors:
    """Test _collect_ancestors IS_A hierarchy traversal.

    All r2 triplets use natural direction: head=child, tail=parent.
    """

    def _make_entities(self, ids_and_types):
        return pd.DataFrame(
            [
                {"foodatlas_id": fid, "entity_type": etype}
                for fid, etype in ids_and_types
            ]
        )

    def test_direct_parent(self):
        r2 = pd.DataFrame([{"head_id": "c1", "tail_id": "p1"}])
        entities = self._make_entities([("c1", "chemical"), ("p1", "chemical")])
        result = _collect_ancestors(r2, {"c1"}, entities)
        assert result == {"p1"}

    def test_transitive_ancestors(self):
        r2 = pd.DataFrame(
            [
                {"head_id": "c1", "tail_id": "p1"},
                {"head_id": "p1", "tail_id": "gp1"},
            ]
        )
        entities = self._make_entities(
            [
                ("c1", "chemical"),
                ("p1", "chemical"),
                ("gp1", "chemical"),
            ]
        )
        result = _collect_ancestors(r2, {"c1"}, entities)
        assert result == {"p1", "gp1"}

    def test_ignores_non_chemical(self):
        r2 = pd.DataFrame([{"head_id": "c1", "tail_id": "d1"}])
        entities = self._make_entities([("c1", "chemical"), ("d1", "disease")])
        result = _collect_ancestors(r2, {"c1"}, entities)
        assert result == set()

    def test_no_parents(self):
        r2 = pd.DataFrame(columns=["head_id", "tail_id"])
        entities = self._make_entities([("c1", "chemical")])
        result = _collect_ancestors(r2, {"c1"}, entities)
        assert result == set()

    def test_seed_not_in_hierarchy(self):
        r2 = pd.DataFrame([{"head_id": "c2", "tail_id": "p1"}])
        entities = self._make_entities(
            [
                ("c1", "chemical"),
                ("c2", "chemical"),
                ("p1", "chemical"),
            ]
        )
        result = _collect_ancestors(r2, {"c1"}, entities)
        assert result == set()
