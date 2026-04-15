"""Tests for src.etl.materializer_correlation — pure helper functions."""

import json
from unittest.mock import MagicMock

import pandas as pd
from src.etl.materializer_correlation import (
    _build_ancestors,
    _get_correlation_evidence,
    materialize_chemical_disease_correlation,
)


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


class TestBuildAncestors:
    """Verify r2 direction: head=child, tail=parent (natural)."""

    def test_leaf_walks_up_to_ancestors(self, monkeypatch):
        # Chain: c is_a p is_a gp. Natural direction: head=child, tail=parent.
        monkeypatch.setattr(
            "src.etl.materializer_correlation.pd.read_sql",
            lambda _q, _c: pd.DataFrame(
                [("c", "p"), ("p", "gp")], columns=["head_id", "tail_id"]
            ),
        )
        etype = {"gp": "chemical", "p": "chemical", "c": "chemical"}
        ancestors_of = _build_ancestors(MagicMock(), etype)
        assert ancestors_of["c"] == {"p", "gp"}
        assert ancestors_of["p"] == {"gp"}

    def test_non_chemical_edges_ignored(self, monkeypatch):
        # Natural direction: head=child, tail=parent.
        monkeypatch.setattr(
            "src.etl.materializer_correlation.pd.read_sql",
            lambda _q, _c: pd.DataFrame(
                [("c", "p"), ("dchild", "dparent")],
                columns=["head_id", "tail_id"],
            ),
        )
        etype = {
            "p": "chemical",
            "c": "chemical",
            "dparent": "disease",
            "dchild": "disease",
        }
        ancestors_of = _build_ancestors(MagicMock(), etype)
        assert ancestors_of.get("c") == {"p"}
        assert "dchild" not in ancestors_of


class TestFoodScopedInheritance:
    """Inherited rows only fire for food-connected descendants (issue #103)."""

    @staticmethod
    def _fake_read_sql(responses: dict[str, pd.DataFrame]):
        def reader(query, _conn):
            sql = str(query)
            for key, df in responses.items():
                if key in sql:
                    return df.copy()
            return pd.DataFrame()

        return reader

    def test_non_food_descendant_does_not_propagate(self, monkeypatch):
        # Chain: ancestor A -> direct child C. C has an r3 edge to disease D.
        # C is NOT in r1 tails (no food), so A should see nothing inherited.
        responses = {
            "WHERE relationship_id IN ('r3', 'r4')": pd.DataFrame(
                [
                    {
                        "head_id": "c_chem",
                        "tail_id": "d_disease",
                        "relationship_id": "r3",
                        "attestation_ids": [],
                    }
                ]
            ),
            "FROM base_attestations": pd.DataFrame(
                columns=["attestation_id", "evidence_id", "source"]
            ),
            "FROM base_evidence": pd.DataFrame(columns=["evidence_id", "reference"]),
            "FROM base_entities": pd.DataFrame(
                [
                    {
                        "foodatlas_id": "a_chem",
                        "common_name": "A",
                        "entity_type": "chemical",
                    },
                    {
                        "foodatlas_id": "c_chem",
                        "common_name": "C",
                        "entity_type": "chemical",
                    },
                    {
                        "foodatlas_id": "d_disease",
                        "common_name": "D",
                        "entity_type": "disease",
                    },
                ]
            ),
            # No r1 rows → food_chem_ids is empty
            "WHERE relationship_id = 'r1'": pd.DataFrame(columns=["tail_id"]),
            # Natural r2: c_chem is_a a_chem (head=child, tail=parent).
            "WHERE relationship_id = 'r2'": pd.DataFrame(
                [{"head_id": "c_chem", "tail_id": "a_chem"}]
            ),
        }
        monkeypatch.setattr(
            "src.etl.materializer_correlation.pd.read_sql",
            self._fake_read_sql(responses),
        )
        captured: list[pd.DataFrame] = []
        monkeypatch.setattr(
            "src.etl.materializer_correlation.bulk_copy",
            lambda _c, _t, df, _cols: captured.append(df.copy()),
        )

        materialize_chemical_disease_correlation(MagicMock())

        assert captured, "expected at least one bulk_copy call"
        df = captured[0]
        # Only the direct row (C, C, D) should exist. No (A, C, D) row.
        assert len(df) == 1
        assert df.iloc[0]["chemical_foodatlas_id"] == "c_chem"
        assert df.iloc[0]["source_chemical_foodatlas_id"] == "c_chem"

    def test_food_descendant_propagates(self, monkeypatch):
        # Same chain but C IS in r1 tails → inherited row on A should fire.
        responses = {
            "WHERE relationship_id IN ('r3', 'r4')": pd.DataFrame(
                [
                    {
                        "head_id": "c_chem",
                        "tail_id": "d_disease",
                        "relationship_id": "r3",
                        "attestation_ids": [],
                    }
                ]
            ),
            "FROM base_attestations": pd.DataFrame(
                columns=["attestation_id", "evidence_id", "source"]
            ),
            "FROM base_evidence": pd.DataFrame(columns=["evidence_id", "reference"]),
            "FROM base_entities": pd.DataFrame(
                [
                    {
                        "foodatlas_id": "a_chem",
                        "common_name": "A",
                        "entity_type": "chemical",
                    },
                    {
                        "foodatlas_id": "c_chem",
                        "common_name": "C",
                        "entity_type": "chemical",
                    },
                    {
                        "foodatlas_id": "d_disease",
                        "common_name": "D",
                        "entity_type": "disease",
                    },
                ]
            ),
            # C is in r1 tail → food-connected
            "WHERE relationship_id = 'r1'": pd.DataFrame([{"tail_id": "c_chem"}]),
            # Natural r2: c_chem is_a a_chem (head=child, tail=parent).
            "WHERE relationship_id = 'r2'": pd.DataFrame(
                [{"head_id": "c_chem", "tail_id": "a_chem"}]
            ),
        }
        monkeypatch.setattr(
            "src.etl.materializer_correlation.pd.read_sql",
            self._fake_read_sql(responses),
        )
        captured: list[pd.DataFrame] = []
        monkeypatch.setattr(
            "src.etl.materializer_correlation.bulk_copy",
            lambda _c, _t, df, _cols: captured.append(df.copy()),
        )

        materialize_chemical_disease_correlation(MagicMock())

        df = captured[0]
        assert len(df) == 2
        chem_ids = set(df["chemical_foodatlas_id"])
        assert chem_ids == {"a_chem", "c_chem"}
        # Both rows attribute to source=C
        assert set(df["source_chemical_foodatlas_id"]) == {"c_chem"}
