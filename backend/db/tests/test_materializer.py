"""Tests for materializer helper functions."""

from unittest.mock import MagicMock

import pandas as pd
from src.etl.materializer import _insert_mv_entities
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
