"""Tests for materializer helper functions (composition + classification)."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.materializer import _build_classification_map
from src.etl.materializer_composition import (
    _add_foodatlas_evidence,
    _build_evidence_json,
    _compute_median,
    materialize_food_chemical_composition,
)


class TestBuildClassificationMap:
    def test_returns_matching_labels(self):
        r2 = pd.DataFrame(
            {
                "head_id": ["e1", "e1", "e2"],
                "tail_id": ["e10", "e11", "e10"],
                "source": ["foodon", "foodon", "chebi"],
            }
        )
        name_map = {"e10": "fruit", "e11": "vegetable", "e12": "organic"}
        result = _build_classification_map(r2, name_map, "foodon")
        assert result["e1"] == ["fruit", "vegetable"]

    def test_returns_empty_for_no_match(self):
        r2 = pd.DataFrame(
            {
                "head_id": ["e2"],
                "tail_id": ["e10"],
                "source": ["chebi"],
            }
        )
        name_map = {"e10": "x"}
        result = _build_classification_map(r2, name_map, "foodon")
        assert "e1" not in result

    def test_skips_missing_tail_ids(self):
        r2 = pd.DataFrame(
            {
                "head_id": ["e1"],
                "tail_id": ["e999"],
                "source": ["foodon"],
            }
        )
        name_map = {"e10": "x"}
        result = _build_classification_map(r2, name_map, "foodon")
        assert result.get("e1") == []


class TestComputeMedian:
    def test_returns_median(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 10.0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 20.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result == {"unit": "mg/100g", "value": "15"}

    def test_returns_none_for_empty(self):
        assert _compute_median([]) is None

    def test_skips_zero_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 5.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median(evidences)
        assert result is not None
        assert result["value"] == "5"

    def test_skips_non_mg100g_units(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 10.0, "unit": "ppm"}},
                ]
            },
        ]
        assert _compute_median(evidences) is None

    def test_skips_none_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": None, "unit": "mg/100g"}},
                ]
            },
        ]
        assert _compute_median(evidences) is None


class TestAddFoodatlasEvidence:
    def test_creates_new_evidence(self):
        evidences: list = []
        ref = {"pmcid": "PMC123", "text": "Some premise"}
        extraction = {"method": "lit2kg:gpt-3.5-ft"}
        _add_foodatlas_evidence(evidences, ref, extraction)
        assert len(evidences) == 1
        assert evidences[0]["premise"] == "Some premise"
        assert evidences[0]["reference"]["id"] == "PMC123"

    def test_groups_by_premise(self):
        evidences: list = []
        ref = {"pmcid": "PMC123", "text": "Same premise"}
        _add_foodatlas_evidence(evidences, ref, {"method": "m1"})
        _add_foodatlas_evidence(evidences, ref, {"method": "m2"})
        assert len(evidences) == 1
        assert len(evidences[0]["extraction"]) == 2

    def test_different_premises_separate(self):
        evidences: list = []
        _add_foodatlas_evidence(
            evidences, {"pmcid": "1", "text": "A"}, {"method": "m1"}
        )
        _add_foodatlas_evidence(
            evidences, {"pmcid": "2", "text": "B"}, {"method": "m2"}
        )
        assert len(evidences) == 2


class TestBuildEvidenceJson:
    def _make_group(self, source, conc_value=10.0):
        return pd.DataFrame(
            [
                {
                    "source": source,
                    "reference": {
                        "url": "https://example.com",
                        "pmcid": "PMC1",
                        "text": "t",
                    },
                    "conc_value": conc_value,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": str(conc_value),
                    "show_food": "apple",
                    "show_chem": "vitamin c",
                }
            ]
        )

    def test_fdc_grouped(self):
        group = self._make_group("fdc")
        result = _build_evidence_json(group)
        assert result["fdc"] is not None
        assert len(result["fdc"]) == 1
        assert result["foodatlas"] is None
        assert result["dmd"] is None

    def test_dmd_grouped(self):
        group = self._make_group("dmd")
        result = _build_evidence_json(group)
        assert result["dmd"] is not None
        assert result["fdc"] is None

    def test_foodatlas_grouped(self):
        group = self._make_group("lit2kg")
        result = _build_evidence_json(group)
        assert result["foodatlas"] is not None
        assert result["fdc"] is None

    def test_mixed_sources(self):
        group = pd.concat(
            [
                self._make_group("fdc"),
                self._make_group("lit2kg", 5.0),
            ],
            ignore_index=True,
        )
        result = _build_evidence_json(group)
        assert result["fdc"] is not None
        assert result["foodatlas"] is not None

    def test_null_conc_value(self):
        group = self._make_group("fdc", conc_value=float("nan"))
        result = _build_evidence_json(group)
        assert result["fdc"] is not None
        ext = result["fdc"][0]["extraction"][0]
        assert ext["converted_concentration"]["value"] is None

    def test_fdc_reference_id(self):
        group = pd.DataFrame(
            [
                {
                    "source": "fdc",
                    "reference": {"fdc_id": "12345", "url": "https://fdc.example.com"},
                    "conc_value": 10.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "10",
                    "show_food": "apple",
                    "show_chem": "vit c",
                }
            ]
        )
        result = _build_evidence_json(group)
        assert result["fdc"][0]["reference"]["id"] == "12345"
        assert result["fdc"][0]["reference"]["source_name"] == "FDC"

    def test_dmd_reference_id(self):
        group = pd.DataFrame(
            [
                {
                    "source": "dmd",
                    "reference": {
                        "dmd_concentration_id": "dmd99",
                        "url": "https://dmd.com",
                    },
                    "conc_value": 20.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "20",
                    "show_food": "milk",
                    "show_chem": "casein",
                }
            ]
        )
        result = _build_evidence_json(group)
        assert result["dmd"][0]["reference"]["id"] == "dmd99"
        assert result["dmd"][0]["reference"]["source_name"] == "DMD"

    def test_foodatlas_groups_by_premise(self):
        group = pd.DataFrame(
            [
                {
                    "source": "lit2kg",
                    "reference": {"pmcid": "PMC1", "text": "same premise"},
                    "conc_value": 5.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "5",
                    "show_food": "apple",
                    "show_chem": "vit c",
                },
                {
                    "source": "lit2kg",
                    "reference": {"pmcid": "PMC1", "text": "same premise"},
                    "conc_value": 10.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "10",
                    "show_food": "apple",
                    "show_chem": "vit c",
                },
            ]
        )
        result = _build_evidence_json(group)
        assert result["foodatlas"] is not None
        assert len(result["foodatlas"]) == 1
        assert len(result["foodatlas"][0]["extraction"]) == 2

    def test_non_dict_reference(self):
        group = pd.DataFrame(
            [
                {
                    "source": "fdc",
                    "reference": "not_a_dict",
                    "conc_value": 10.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "10",
                    "show_food": "apple",
                    "show_chem": "vit c",
                }
            ]
        )
        result = _build_evidence_json(group)
        assert result["fdc"] is not None

    def test_empty_group_returns_all_none(self):
        group = pd.DataFrame(
            columns=[
                "source",
                "reference",
                "conc_value",
                "conc_unit",
                "conc_value_raw",
                "show_food",
                "show_chem",
            ]
        )
        result = _build_evidence_json(group)
        assert result["fdc"] is None
        assert result["foodatlas"] is None
        assert result["dmd"] is None

    def test_foodatlas_url_with_pmcid(self):
        group = self._make_group("lit2kg")
        result = _build_evidence_json(group)
        url = result["foodatlas"][0]["reference"]["url"]
        assert "PMC1" in url

    def test_foodatlas_empty_pmcid(self):
        group = pd.DataFrame(
            [
                {
                    "source": "lit2kg",
                    "reference": {"pmcid": "", "text": "premise"},
                    "conc_value": 5.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "5",
                    "show_food": "apple",
                    "show_chem": "vit c",
                }
            ]
        )
        result = _build_evidence_json(group)
        assert result["foodatlas"][0]["reference"]["url"] == ""


class TestMaterializeCompositionEmpty:
    """Test materialize_food_chemical_composition with empty data."""

    @patch("src.etl.materializer_composition.bulk_copy")
    @patch("src.etl.materializer_composition.pd.read_sql")
    def test_empty_triplets_produces_no_rows(self, mock_sql, mock_copy):
        """No r1 triplets → no rows inserted."""
        mock_sql.side_effect = [
            pd.DataFrame(columns=["head_id", "tail_id", "attestation_ids"]),
            pd.DataFrame(
                columns=[
                    "attestation_id",
                    "evidence_id",
                    "source",
                    "head_name_raw",
                    "tail_name_raw",
                    "conc_value",
                    "conc_unit",
                    "conc_value_raw",
                    "conc_unit_raw",
                ]
            ),
            pd.DataFrame(columns=["evidence_id", "source_type", "reference"]),
            pd.DataFrame(columns=["foodatlas_id", "common_name"]),
            pd.DataFrame(columns=["foodatlas_id", "nutrient_classification"]),
        ]
        conn = MagicMock()
        materialize_food_chemical_composition(conn)
        mock_copy.assert_not_called()
