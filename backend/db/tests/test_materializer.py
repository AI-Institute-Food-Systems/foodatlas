"""Tests for src.etl.materializer — pure helper functions."""

import pandas as pd
from src.etl.materializer import (
    _add_foodatlas_evidence,
    _build_composition_row,
    _build_db_evidence,
    _compute_median_concentration,
    _get_classifications,
    _group_evidences,
)


class TestComputeMedianConcentration:
    """Test _compute_median_concentration with various inputs."""

    def test_single_value(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 10.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median_concentration(evidences)
        assert result is not None
        assert result["unit"] == "mg/100g"
        assert result["value"] == "10"

    def test_multiple_values_median(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 2.0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 8.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median_concentration(evidences)
        assert result is not None
        assert result["value"] == "5"

    def test_skips_zero_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 0.0, "unit": "mg/100g"}},
                    {"converted_concentration": {"value": 6.0, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median_concentration(evidences)
        assert result is not None
        assert result["value"] == "6"

    def test_skips_non_mg_units(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": 5.0, "unit": "g/100g"}},
                ]
            },
        ]
        result = _compute_median_concentration(evidences)
        assert result is None

    def test_skips_none_values(self):
        evidences = [
            {
                "extraction": [
                    {"converted_concentration": {"value": None, "unit": "mg/100g"}},
                ]
            },
        ]
        result = _compute_median_concentration(evidences)
        assert result is None

    def test_empty_evidences(self):
        result = _compute_median_concentration([])
        assert result is None

    def test_no_extraction_key(self):
        evidences = [{"other": "data"}]
        result = _compute_median_concentration(evidences)
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
        result = _compute_median_concentration(evidences)
        assert result is not None
        assert result["value"] == "2.5"


class TestBuildDbEvidence:
    """Test _build_db_evidence for FDC and DMD sources."""

    def test_fdc_evidence(self):
        ref = {"fdc_id": 12345, "url": "https://fdc.example.com/12345"}
        extraction = {"extracted_food_name": "Apple", "method": "fdc"}
        result = _build_db_evidence("fdc", ref, extraction)
        assert result["premise"] is None
        assert result["reference"]["id"] == "12345"
        assert result["reference"]["source_name"] == "FDC"
        assert result["reference"]["display_name"] == "FDC ID"
        assert result["extraction"] == [extraction]

    def test_dmd_evidence(self):
        ref = {"dmd_concentration_id": "dmd_99", "url": "https://dmd.example.com"}
        extraction = {"extracted_food_name": "Banana", "method": "dmd"}
        result = _build_db_evidence("dmd", ref, extraction)
        assert result["reference"]["id"] == "dmd_99"
        assert result["reference"]["source_name"] == "DMD"

    def test_fdc_missing_fdc_id_falls_back_to_url(self):
        ref = {"url": "https://fallback.example.com"}
        ext: dict[str, str] = {}
        result = _build_db_evidence("fdc", ref, ext)
        assert result["reference"]["id"] == "https://fallback.example.com"

    def test_dmd_missing_id_falls_back_to_url(self):
        ref = {"url": "https://dmd.example.com/fallback"}
        ext: dict[str, str] = {}
        result = _build_db_evidence("dmd", ref, ext)
        assert result["reference"]["id"] == "https://dmd.example.com/fallback"


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


class TestGetClassifications:
    """Test _get_classifications with mock DataFrames."""

    def test_finds_matching_classifications(self):
        r2 = pd.DataFrame(
            [
                {"head_id": "c001", "tail_id": "class1", "source": "chebi_ontology"},
                {"head_id": "c001", "tail_id": "class2", "source": "chebi_other"},
                {"head_id": "c002", "tail_id": "class3", "source": "chebi_ontology"},
            ]
        )
        entity_map = pd.DataFrame(
            {
                "common_name": ["ClassA", "ClassB", "ClassC"],
            },
            index=["class1", "class2", "class3"],
        )
        entity_map.index.name = "foodatlas_id"

        result = _get_classifications("c001", r2, entity_map, "chebi")
        assert result == ["ClassA", "ClassB"]

    def test_no_matching_source(self):
        r2 = pd.DataFrame(
            [
                {"head_id": "c001", "tail_id": "class1", "source": "foodon_ontology"},
            ]
        )
        entity_map = pd.DataFrame({"common_name": ["X"]}, index=["class1"])
        result = _get_classifications("c001", r2, entity_map, "chebi")
        assert result == []

    def test_empty_r2(self):
        r2 = pd.DataFrame(columns=["head_id", "tail_id", "source"])
        entity_map = pd.DataFrame(columns=["common_name"])
        result = _get_classifications("c001", r2, entity_map, "chebi")
        assert result == []

    def test_missing_tail_in_entity_map(self):
        r2 = pd.DataFrame(
            [
                {"head_id": "c001", "tail_id": "missing", "source": "chebi_x"},
            ]
        )
        entity_map = pd.DataFrame({"common_name": ["A"]}, index=["other"])
        result = _get_classifications("c001", r2, entity_map, "chebi")
        assert result == []


class TestGroupEvidences:
    """Test _group_evidences with mock attestation/evidence maps."""

    def _make_maps(self):
        att_data = pd.DataFrame(
            [
                {
                    "attestation_id": "att1",
                    "evidence_id": "ev1",
                    "source": "fdc",
                    "head_name_raw": "A",
                    "tail_name_raw": "B",
                    "conc_value": 5.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "5",
                    "conc_unit_raw": "mg/100 g",
                },
                {
                    "attestation_id": "att2",
                    "evidence_id": "ev2",
                    "source": "lit2kg",
                    "head_name_raw": "",
                    "tail_name_raw": "",
                    "conc_value": None,
                    "conc_unit": "",
                    "conc_value_raw": "",
                    "conc_unit_raw": "",
                },
            ]
        ).set_index("attestation_id")
        ev_data = pd.DataFrame(
            [
                {"evidence_id": "ev1", "reference": {"fdc_id": 1, "url": "u1"}},
                {"evidence_id": "ev2", "reference": {"pmcid": "P1", "text": "t"}},
            ]
        ).set_index("evidence_id")
        return att_data, ev_data

    def test_fdc_evidence_grouped(self):
        att_map, ev_map = self._make_maps()
        fdc, fa, dmd = _group_evidences(["att1"], att_map, ev_map, "Apple", "VitC")
        assert fdc is not None
        assert len(fdc) == 1
        assert fa is None
        assert dmd is None

    def test_foodatlas_evidence_grouped(self):
        att_map, ev_map = self._make_maps()
        fdc, fa, dmd = _group_evidences(["att2"], att_map, ev_map, "Apple", "VitC")
        assert fdc is None
        assert fa is not None
        assert dmd is None

    def test_missing_attestation_skipped(self):
        att_map, ev_map = self._make_maps()
        fdc, fa, dmd = _group_evidences(["nonexistent"], att_map, ev_map, "X", "Y")
        assert fdc is None
        assert fa is None
        assert dmd is None

    def test_zero_conc_skipped(self):
        att_map, ev_map = self._make_maps()
        att_map.loc["att1", "conc_value"] = 0.0
        fdc, _fa, _dmd = _group_evidences(["att1"], att_map, ev_map, "X", "Y")
        assert fdc is None


class TestBuildCompositionRow:
    """Test _build_composition_row with mock data."""

    def test_returns_row_dict(self):
        triplet = pd.Series(
            {
                "head_id": "f001",
                "tail_id": "c001",
                "attestation_ids": ["att1"],
            }
        )
        att_map = pd.DataFrame(
            [
                {
                    "attestation_id": "att1",
                    "evidence_id": "ev1",
                    "source": "fdc",
                    "head_name_raw": "Apple",
                    "tail_name_raw": "VitC",
                    "conc_value": 5.0,
                    "conc_unit": "mg/100g",
                    "conc_value_raw": "5",
                    "conc_unit_raw": "mg/100 g",
                }
            ]
        ).set_index("attestation_id")
        ev_map = pd.DataFrame(
            [
                {
                    "evidence_id": "ev1",
                    "reference": {"fdc_id": 1, "url": "u1"},
                }
            ]
        ).set_index("evidence_id")
        name_map = {"f001": "Apple", "c001": "Vitamin C"}
        nutr_map = {"c001": ["vitamin"]}

        result = _build_composition_row(triplet, name_map, att_map, ev_map, nutr_map)
        assert result is not None
        assert result["food_name"] == "Apple"
        assert result["chemical_name"] == "Vitamin C"
        assert result["nutrient_classification"] == ["vitamin"]

    def test_returns_none_with_no_evidence(self):
        triplet = pd.Series(
            {
                "head_id": "f001",
                "tail_id": "c001",
                "attestation_ids": [],
            }
        )
        result = _build_composition_row(triplet, {}, pd.DataFrame(), pd.DataFrame(), {})
        assert result is None
