"""Tests for src.etl._bioactivity_helpers."""

import pandas as pd
from src.etl._bioactivity_helpers import build_measurement, explode_attestations


class TestExplodeAttestations:
    def test_empty_triplets_returns_empty(self):
        df = pd.DataFrame({"head_id": [], "tail_id": [], "attestation_ids": []})
        result = explode_attestations(df)
        assert result.empty
        assert "attestation_id" in result.columns

    def test_single_attestation_one_row(self):
        df = pd.DataFrame(
            {
                "head_id": ["c1"],
                "tail_id": ["b1"],
                "attestation_ids": [["ba1"]],
            }
        )
        result = explode_attestations(df)
        assert len(result) == 1
        assert result.iloc[0]["attestation_id"] == "ba1"

    def test_multiple_attestations_explodes(self):
        df = pd.DataFrame(
            {
                "head_id": ["c1", "c2"],
                "tail_id": ["b1", "b1"],
                "attestation_ids": [["ba1", "ba2"], ["ba3"]],
            }
        )
        result = explode_attestations(df)
        assert len(result) == 3
        assert set(result["attestation_id"]) == {"ba1", "ba2", "ba3"}

    def test_drops_null_attestation_ids(self):
        df = pd.DataFrame(
            {
                "head_id": ["c1"],
                "tail_id": ["b1"],
                "attestation_ids": [[]],
            }
        )
        result = explode_attestations(df)
        assert result.empty


class TestBuildMeasurement:
    def test_full_payload(self):
        att = {
            "attestation_id": "ba1",
            "bioactivity_metadata_id": "BAM000001",
            "source_assay_id": "PubChem AID:1",
            "target_ids": ["UniProt:P1"],
            "evidence_value_potency_value": 5.0,
            "evidence_value_potency_unit": "uM",
            "evidence_value_efficacy_zeroactivity": 0.5,
            "evidence_value_efficacy_infiniteactivity": -50.0,
            "evidence_value_efficacy_logac50_value": -5.0,
            "evidence_value_efficacy_hillslope": 1.2,
            "evidence_source": "PubChem AID:1",
            "evidence_type": "In vitro",
        }
        result = build_measurement(att)
        assert result["attestation_id"] == "ba1"
        assert result["potency"] == {"value": 5.0, "unit": "uM"}
        assert result["hill_curve"]["zero_activity"] == 0.5
        assert result["hill_curve"]["hill_slope"] == 1.2
        assert result["target_ids"] == ["UniProt:P1"]

    def test_missing_optional_fields(self):
        att = {
            "attestation_id": "ba1",
            "bioactivity_metadata_id": "BDM000001",
            "target_ids": None,
        }
        result = build_measurement(att)
        assert result["target_ids"] == []
        assert result["potency"] == {"value": None, "unit": None}
        assert result["evidence_source"] is None
