"""Tests for src.etl.materializer_bioactivity_bridge."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.materializer_bioactivity_bridge import (
    assay_bioactivity_map,
    attach_literature,
    build_bridge_evidence,
    chemical_active_assays,
    literature_directions,
)

_MODULE = "src.etl.materializer_bioactivity_bridge"


class TestAssayBioactivityMap:
    """Concept codes in base_bioassays resolve via external_ids."""

    def _run(self, assays: pd.DataFrame, entities: pd.DataFrame) -> pd.DataFrame:
        with patch(f"{_MODULE}.pd.read_sql", side_effect=[assays, entities]):
            return assay_bioactivity_map(MagicMock())

    def test_resolves_concept_to_entity(self):
        out = self._run(
            pd.DataFrame([{"source_assay_id": "AID: 1", "bioactivity_ids": ["E1"]}]),
            pd.DataFrame(
                [
                    {
                        "foodatlas_id": "b1",
                        "external_ids": {"bioactivity_concept": ["E1"]},
                    }
                ]
            ),
        )
        assert out.to_dict("records") == [
            {"source_assay_id": "AID: 1", "bioactivity_id": "b1"}
        ]

    def test_explodes_multi_bioactivity_assays(self):
        out = self._run(
            pd.DataFrame(
                [{"source_assay_id": "AID: 1", "bioactivity_ids": ["E1", "E2"]}]
            ),
            pd.DataFrame(
                [
                    {
                        "foodatlas_id": "b1",
                        "external_ids": {"bioactivity_concept": ["E1"]},
                    },
                    {
                        "foodatlas_id": "b2",
                        "external_ids": {"bioactivity_concept": ["E2"]},
                    },
                ]
            ),
        )
        assert set(out["bioactivity_id"]) == {"b1", "b2"}

    def test_drops_unmapped_concepts(self):
        """An unknown concept is dropped, not carried through as a null id."""
        out = self._run(
            pd.DataFrame(
                [
                    {"source_assay_id": "AID: 1", "bioactivity_ids": ["E1"]},
                    {"source_assay_id": "AID: 2", "bioactivity_ids": ["E_UNKNOWN"]},
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "foodatlas_id": "b1",
                        "external_ids": {"bioactivity_concept": ["E1"]},
                    }
                ]
            ),
        )
        assert out["source_assay_id"].tolist() == ["AID: 1"]

    def test_empty_assays_returns_empty_frame(self):
        empty = pd.DataFrame(columns=["source_assay_id", "bioactivity_ids"])
        with patch(f"{_MODULE}.pd.read_sql", return_value=empty):
            out = assay_bioactivity_map(MagicMock())
        assert out.empty
        assert list(out.columns) == ["source_assay_id", "bioactivity_id"]


class TestChemicalActiveAssays:
    def test_joins_r6_attestations_to_active_measurements(self):
        r6 = pd.DataFrame([{"head_id": "c1", "attestation_ids": ["bm1", "bm2"]}])
        active = pd.DataFrame([{"bm": "bm1", "source_assay_id": "AID: 1"}])
        with patch(f"{_MODULE}.pd.read_sql", side_effect=[r6, active]):
            out = chemical_active_assays(MagicMock())
        # bm2 has no Active measurement, so it drops out.
        assert out.to_dict("records") == [
            {"chemical_id": "c1", "source_assay_id": "AID: 1", "bm": "bm1"}
        ]


class TestBuildBridgeEvidence:
    def test_empty_bridge_returns_empty(self):
        bridge = pd.DataFrame(
            columns=[
                "disease_mesh_id",
                "source_assay_id",
                "relationship",
                "bioactivity_disease_metadata_id",
            ]
        )
        r6 = pd.DataFrame([{"head_id": "c1", "attestation_ids": ["bm1"]}])
        active = pd.DataFrame([{"bm": "bm1", "source_assay_id": "AID: 1"}])
        with patch(f"{_MODULE}.pd.read_sql", side_effect=[bridge, r6, active]):
            evidence, names = build_bridge_evidence(MagicMock())
        assert evidence.empty
        assert names == {}

    def test_maps_mesh_to_disease_and_drops_unmatched(self):
        bridge = pd.DataFrame(
            [
                {
                    "disease_mesh_id": "MESH:D1",
                    "source_assay_id": "AID: 1",
                    "relationship": ["therapeutic"],
                    "bioactivity_disease_metadata_id": ["bdm1"],
                },
                {
                    "disease_mesh_id": "MESH:UNKNOWN",
                    "source_assay_id": "AID: 1",
                    "relationship": ["therapeutic"],
                    "bioactivity_disease_metadata_id": ["bdm2"],
                },
            ]
        )
        r6 = pd.DataFrame([{"head_id": "c1", "attestation_ids": ["bm1"]}])
        active = pd.DataFrame([{"bm": "bm1", "source_assay_id": "AID: 1"}])
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "d1",
                    "entity_type": "disease",
                    "common_name": "melanoma",
                    "external_ids": {"ctd": ["MESH:D1"]},
                }
            ]
        )
        with patch(
            f"{_MODULE}.pd.read_sql", side_effect=[bridge, r6, active, entities]
        ):
            evidence, names = build_bridge_evidence(MagicMock())
        assert evidence["disease_id"].tolist() == ["d1"]
        assert names["d1"] == "melanoma"


class TestLiteratureDirections:
    """CTD relationship ids map onto the assay bridge's own vocabulary."""

    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        with patch(f"{_MODULE}.pd.read_sql", return_value=df):
            return literature_directions(MagicMock())

    def test_maps_r3_and_r4_to_bridge_vocabulary(self):
        out = self._run(
            pd.DataFrame(
                [
                    {
                        "chemical_id": "c1",
                        "disease_id": "d1",
                        "relationship_id": "r3",
                    },
                    {
                        "chemical_id": "c1",
                        "disease_id": "d1",
                        "relationship_id": "r4",
                    },
                ]
            )
        )
        assert out["literature_directions"].tolist() == [
            ["marker/mechanism", "therapeutic"]
        ]

    def test_drops_unmapped_relationship_ids(self):
        # r1/r2 are composition edges — they say nothing about disease direction.
        out = self._run(
            pd.DataFrame(
                [
                    {
                        "chemical_id": "c1",
                        "disease_id": "d1",
                        "relationship_id": "r1",
                    }
                ]
            )
        )
        assert out.empty

    def test_empty_input_returns_typed_frame(self):
        out = self._run(pd.DataFrame())
        assert list(out.columns) == [
            "chemical_id",
            "disease_id",
            "literature_directions",
        ]


class TestAttachLiterature:
    def test_pairs_without_literature_get_empty_list(self):
        out = attach_literature(
            pd.DataFrame(
                [
                    {"chemical_id": "c1", "disease_id": "d1"},
                    {"chemical_id": "c2", "disease_id": "d2"},
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "chemical_id": "c1",
                        "disease_id": "d1",
                        "literature_directions": ["therapeutic"],
                    }
                ]
            ),
        )
        assert out["literature_directions"].tolist() == [["therapeutic"], []]

    def test_no_literature_at_all_still_yields_the_column(self):
        out = attach_literature(
            pd.DataFrame([{"chemical_id": "c1", "disease_id": "d1"}]),
            pd.DataFrame(
                columns=["chemical_id", "disease_id", "literature_directions"]
            ),
        )
        assert out["literature_directions"].tolist() == [[]]
