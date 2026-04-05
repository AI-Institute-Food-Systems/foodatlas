"""Tests for src.etl.materializer_correlation — pure helper functions."""

import json

import pandas as pd
from src.etl.materializer_correlation import _get_correlation_evidence


class TestGetCorrelationEvidence:
    """Test _get_correlation_evidence with mock DataFrames."""

    def _make_maps(self):
        att_data = pd.DataFrame(
            [
                {
                    "attestation_id": "att1",
                    "evidence_id": "ev1",
                    "source": "lit2kg",
                },
                {
                    "attestation_id": "att2",
                    "evidence_id": "ev2",
                    "source": "ctd",
                },
            ]
        ).set_index("attestation_id")

        ev_data = pd.DataFrame(
            [
                {
                    "evidence_id": "ev1",
                    "reference": {
                        "pmcid": "PMC111",
                        "pmid": "222",
                    },
                },
                {
                    "evidence_id": "ev2",
                    "reference": {"pmcid": "PMC333"},
                },
            ]
        ).set_index("evidence_id")

        return att_data, ev_data

    def test_extracts_sources(self):
        att_map, ev_map = self._make_maps()
        sources, _ = _get_correlation_evidence(["att1", "att2"], att_map, ev_map)
        assert set(sources) == {"lit2kg", "ctd"}

    def test_extracts_pmcid_and_pmid(self):
        att_map, ev_map = self._make_maps()
        _, evidences = _get_correlation_evidence(["att1"], att_map, ev_map)
        assert len(evidences) == 1
        ev = evidences[0]
        assert "pmcid" in ev
        assert ev["pmcid"]["id"] == "PMC111"
        assert "PMC111" in ev["pmcid"]["url"]
        assert "pmid" in ev
        assert ev["pmid"]["id"] == "222"
        assert "222" in ev["pmid"]["url"]

    def test_pmcid_only(self):
        att_map, ev_map = self._make_maps()
        _, evidences = _get_correlation_evidence(["att2"], att_map, ev_map)
        assert len(evidences) == 1
        assert "pmcid" in evidences[0]
        assert "pmid" not in evidences[0]

    def test_missing_attestation_skipped(self):
        att_map, ev_map = self._make_maps()
        sources, evidences = _get_correlation_evidence(["nonexistent"], att_map, ev_map)
        assert sources == []
        assert evidences == []

    def test_missing_evidence_skipped(self):
        att_data = pd.DataFrame(
            [
                {
                    "attestation_id": "att_no_ev",
                    "evidence_id": "ev_missing",
                    "source": "lit2kg",
                }
            ]
        ).set_index("attestation_id")
        ev_data = pd.DataFrame(columns=["evidence_id", "reference"]).set_index(
            "evidence_id"
        )

        sources, evidences = _get_correlation_evidence(["att_no_ev"], att_data, ev_data)
        assert sources == ["lit2kg"]
        assert evidences == []

    def test_empty_att_ids(self):
        att_map, ev_map = self._make_maps()
        sources, evidences = _get_correlation_evidence([], att_map, ev_map)
        assert sources == []
        assert evidences == []

    def test_reference_as_json_string(self):
        """When reference is stored as JSON string instead of dict."""
        att_data = pd.DataFrame(
            [
                {
                    "attestation_id": "att_str",
                    "evidence_id": "ev_str",
                    "source": "ctd",
                }
            ]
        ).set_index("attestation_id")
        ev_data = pd.DataFrame(
            [
                {
                    "evidence_id": "ev_str",
                    "reference": json.dumps({"pmcid": "PMC444"}),
                }
            ]
        ).set_index("evidence_id")

        _, evidences = _get_correlation_evidence(["att_str"], att_data, ev_data)
        assert len(evidences) == 1
        assert evidences[0]["pmcid"]["id"] == "PMC444"

    def test_reference_without_pmcid_or_pmid(self):
        """Evidence without pmcid or pmid produces empty ev_dict."""
        att_data = pd.DataFrame(
            [
                {
                    "attestation_id": "att_empty",
                    "evidence_id": "ev_empty",
                    "source": "other",
                }
            ]
        ).set_index("attestation_id")
        ev_data = pd.DataFrame(
            [
                {
                    "evidence_id": "ev_empty",
                    "reference": {"url": "https://example.com"},
                }
            ]
        ).set_index("evidence_id")

        _, evidences = _get_correlation_evidence(["att_empty"], att_data, ev_data)
        # No pmcid or pmid in reference, so ev_dict is empty and not appended
        assert evidences == []
