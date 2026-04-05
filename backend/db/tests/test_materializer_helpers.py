"""Tests for materializer helper functions."""

import pandas as pd
from src.etl.materializer import (
    _add_foodatlas_evidence,
    _build_classification_map,
    _build_composition_row,
    _build_db_evidence,
    _compute_median_concentration,
    _group_evidences,
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


class TestComputeMedianConcentration:
    def test_returns_median(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 10.0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 20.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median_concentration(evidences)
        assert result == {"unit": "mg/100g", "value": "15"}

    def test_returns_none_for_empty(self):
        assert _compute_median_concentration([]) is None

    def test_skips_zero_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 5.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median_concentration(evidences)
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
        assert _compute_median_concentration(evidences) is None

    def test_skips_none_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": None, "unit": "mg/100g"}},
                ]
            },
        ]
        assert _compute_median_concentration(evidences) is None


class TestBuildDbEvidence:
    def test_fdc_evidence(self):
        ref = {"fdc_id": "12345", "url": "https://fdc.example.com/12345"}
        extraction = {"method": "fdc"}
        result = _build_db_evidence("fdc", ref, extraction)
        assert result["reference"]["source_name"] == "FDC"
        assert result["reference"]["id"] == "12345"
        assert result["premise"] is None

    def test_dmd_evidence(self):
        ref = {"dmd_concentration_id": "dmd99", "url": "https://dmd.example.com"}
        extraction = {"method": "dmd"}
        result = _build_db_evidence("dmd", ref, extraction)
        assert result["reference"]["source_name"] == "DMD"
        assert result["reference"]["id"] == "dmd99"


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


class TestGroupEvidences:
    def _make_att_map(self):
        return pd.DataFrame(
            {
                "evidence_id": ["ev1", "ev2", "ev3"],
                "source": ["fdc", "lit2kg:gpt-3.5-ft", "dmd"],
                "head_name_raw": ["food1", "food1", "food1"],
                "tail_name_raw": ["chem1", "chem1", "chem1"],
                "conc_value": [10.0, 5.0, 20.0],
                "conc_unit": ["mg/100g", "mg/100g", "mg/100g"],
                "conc_value_raw": ["10", "5", "20"],
            },
            index=["at1", "at2", "at3"],
        )

    def _make_ev_map(self):
        return pd.DataFrame(
            {
                "reference": [
                    {"url": "https://fdc.example.com/1"},
                    {"pmcid": "PMC1", "text": "premise"},
                    {"dmd_concentration_id": "d1", "url": "https://dmd.com"},
                ],
            },
            index=["ev1", "ev2", "ev3"],
        )

    def test_groups_by_source(self):
        att_map = self._make_att_map()
        ev_map = self._make_ev_map()
        fdc, fa, dmd = _group_evidences(
            ["at1", "at2", "at3"], att_map, ev_map, "food1", "chem1"
        )
        assert fdc is not None and len(fdc) == 1
        assert fa is not None and len(fa) == 1
        assert dmd is not None and len(dmd) == 1

    def test_returns_none_for_empty_groups(self):
        att_map = pd.DataFrame(
            {
                "evidence_id": ["ev1"],
                "source": ["fdc"],
                "head_name_raw": ["f"],
                "tail_name_raw": ["c"],
                "conc_value": [10.0],
                "conc_unit": ["mg/100g"],
                "conc_value_raw": ["10"],
            },
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {
                "reference": [{"url": "x"}],
            },
            index=["ev1"],
        )
        fdc, fa, dmd = _group_evidences(["at1"], att_map, ev_map, "f", "c")
        assert fdc is not None
        assert fa is None
        assert dmd is None

    def test_skips_zero_concentration(self):
        att_map = pd.DataFrame(
            {
                "evidence_id": ["ev1"],
                "source": ["fdc"],
                "head_name_raw": ["f"],
                "tail_name_raw": ["c"],
                "conc_value": [0.0],
                "conc_unit": ["mg/100g"],
                "conc_value_raw": ["0"],
            },
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {
                "reference": [{"url": "x"}],
            },
            index=["ev1"],
        )
        fdc, _fa, _dmd = _group_evidences(["at1"], att_map, ev_map, "f", "c")
        assert fdc is None

    def test_skips_missing_attestation(self):
        att_map = pd.DataFrame(
            columns=[
                "evidence_id",
                "source",
                "head_name_raw",
                "tail_name_raw",
                "conc_value",
                "conc_unit",
                "conc_value_raw",
            ]
        )
        ev_map = pd.DataFrame(columns=["reference"])
        fdc, _fa, _dmd = _group_evidences(["missing"], att_map, ev_map, "f", "c")
        assert fdc is None


class TestBuildCompositionRow:
    def test_builds_valid_row(self):
        triplet = pd.Series(
            {
                "head_id": "e1",
                "tail_id": "e2",
                "attestation_ids": ["at1"],
            }
        )
        name_map = {"e1": "tomato", "e2": "vitamin c"}
        att_map = pd.DataFrame(
            {
                "evidence_id": ["ev1"],
                "source": ["fdc"],
                "head_name_raw": ["tomato"],
                "tail_name_raw": ["vit c"],
                "conc_value": [50.0],
                "conc_unit": ["mg/100g"],
                "conc_value_raw": ["50"],
            },
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {
                "reference": [{"url": "https://fdc.example.com/1"}],
            },
            index=["ev1"],
        )
        nutr_map = {"e2": ["vitamin"]}

        row = _build_composition_row(triplet, name_map, att_map, ev_map, nutr_map)
        assert row is not None
        assert row["food_name"] == "tomato"
        assert row["chemical_name"] == "vitamin c"
        assert row["nutrient_classification"] == ["vitamin"]

    def test_returns_none_for_no_evidence(self):
        triplet = pd.Series(
            {
                "head_id": "e1",
                "tail_id": "e2",
                "attestation_ids": [],
            }
        )
        row = _build_composition_row(
            triplet,
            {"e1": "a", "e2": "b"},
            pd.DataFrame(),
            pd.DataFrame(),
            {},
        )
        assert row is None
