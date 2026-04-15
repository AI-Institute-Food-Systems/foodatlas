"""Tests for materializer_correlation helper functions."""

import json

import pandas as pd
from src.etl.materializer_correlation import (
    _deduplicate_evidences,
    _get_correlation_evidence,
)


class TestGetCorrelationEvidence:
    def test_extracts_pmcid_and_pmid(self):
        att_map = pd.DataFrame(
            {"evidence_id": ["ev1"], "source": ["ctd"]},
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {"reference": [{"pmcid": "PMC123", "pmid": "456"}]},
            index=["ev1"],
        )
        sources, evidences = _get_correlation_evidence(["at1"], att_map, ev_map)
        assert "ctd" in sources
        assert len(evidences) == 1
        assert evidences[0]["pmcid"]["id"] == "PMC123"
        assert evidences[0]["pmid"]["id"] == "456"

    def test_handles_pmcid_only(self):
        att_map = pd.DataFrame(
            {"evidence_id": ["ev1"], "source": ["ctd"]},
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {"reference": [{"pmcid": "PMC999"}]},
            index=["ev1"],
        )
        _sources, evidences = _get_correlation_evidence(["at1"], att_map, ev_map)
        assert len(evidences) == 1
        assert "pmcid" in evidences[0]
        assert "pmid" not in evidences[0]

    def test_handles_scalar_pmid(self):
        """CTD evidence has one pmid per evidence record after explode."""
        att_map = pd.DataFrame(
            {"evidence_id": ["ev1"], "source": ["ctd"]},
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {"reference": [{"ctd_direct_evidence": "therapeutic", "pmid": "123"}]},
            index=["ev1"],
        )
        sources, evidences = _get_correlation_evidence(["at1"], att_map, ev_map)
        assert "ctd" in sources
        assert len(evidences) == 1
        assert evidences[0]["pmid"]["id"] == "123"
        assert "pubmed.ncbi" in evidences[0]["pmid"]["url"]

    def test_skips_missing_attestation(self):
        att_map = pd.DataFrame(columns=["evidence_id", "source"])
        ev_map = pd.DataFrame(columns=["reference"])
        sources, evidences = _get_correlation_evidence(["missing"], att_map, ev_map)
        assert sources == []
        assert evidences == []

    def test_skips_missing_evidence(self):
        att_map = pd.DataFrame(
            {"evidence_id": ["missing_ev"], "source": ["ctd"]},
            index=["at1"],
        )
        ev_map = pd.DataFrame(columns=["reference"])
        sources, evidences = _get_correlation_evidence(["at1"], att_map, ev_map)
        assert "ctd" in sources
        assert evidences == []

    def test_handles_string_reference(self):
        att_map = pd.DataFrame(
            {"evidence_id": ["ev1"], "source": ["ctd"]},
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {"reference": [json.dumps({"pmid": "789"})]},
            index=["ev1"],
        )
        _sources, evidences = _get_correlation_evidence(["at1"], att_map, ev_map)
        assert len(evidences) == 1
        assert evidences[0]["pmid"]["id"] == "789"

    def test_skips_evidence_without_pmid_or_pmcid(self):
        att_map = pd.DataFrame(
            {"evidence_id": ["ev1"], "source": ["ctd"]},
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {"reference": [{"other_field": "value"}]},
            index=["ev1"],
        )
        sources, evidences = _get_correlation_evidence(["at1"], att_map, ev_map)
        assert "ctd" in sources
        assert evidences == []

    def test_multiple_attestations(self):
        att_map = pd.DataFrame(
            {"evidence_id": ["ev1", "ev2"], "source": ["ctd", "mesh"]},
            index=["at1", "at2"],
        )
        ev_map = pd.DataFrame(
            {"reference": [{"pmid": "1"}, {"pmcid": "PMC2"}]},
            index=["ev1", "ev2"],
        )
        sources, evidences = _get_correlation_evidence(["at1", "at2"], att_map, ev_map)
        assert set(sources) == {"ctd", "mesh"}
        assert len(evidences) == 2

    def test_pmid_coerced_to_string(self):
        """Numeric pmid values should be coerced to strings."""
        att_map = pd.DataFrame(
            {"evidence_id": ["ev1"], "source": ["ctd"]},
            index=["at1"],
        )
        ev_map = pd.DataFrame(
            {"reference": [{"pmid": 12345}]},
            index=["ev1"],
        )
        _sources, evidences = _get_correlation_evidence(["at1"], att_map, ev_map)
        assert len(evidences) == 1
        assert evidences[0]["pmid"]["id"] == "12345"


class TestDeduplicateEvidences:
    def test_removes_duplicate_pmids(self):
        evidences = [
            {"pmid": {"id": "123", "url": "u1"}},
            {"pmid": {"id": "123", "url": "u1"}},
            {"pmid": {"id": "456", "url": "u2"}},
        ]
        result = _deduplicate_evidences(evidences)
        assert len(result) == 2

    def test_keeps_different_types(self):
        evidences = [
            {"pmid": {"id": "123", "url": "u1"}},
            {"pmcid": {"id": "PMC456", "url": "u2"}},
        ]
        result = _deduplicate_evidences(evidences)
        assert len(result) == 2

    def test_empty_list(self):
        assert _deduplicate_evidences([]) == []
