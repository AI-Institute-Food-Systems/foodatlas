"""Tests for src.etl.materializer_disease_bioactivity."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.materializer_bioactivity_bridge import bioactivity_concept_map
from src.etl.materializer_disease_bioactivity import (
    _aggregate,
    _relationships_per_row,
    materialize_disease_bioactivity,
)

_NAMES = {
    "d1": "melanoma",
    "b1": "anticancer",
    "b2": "antiviral",
    "c1": "quercetin",
}


_NO_LITERATURE = pd.DataFrame(
    columns=["chemical_id", "disease_id", "literature_directions"]
)


def _evidence(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # Every real bridge row carries bdm ids; default them so each test only has
    # to spell out the columns it actually exercises.
    if "bioactivity_disease_metadata_id" not in df.columns and not df.empty:
        df["bioactivity_disease_metadata_id"] = [[] for _ in range(len(df))]
    return df


def _agg(
    evidence: pd.DataFrame,
    target_map: dict | None = None,
    lit: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """_aggregate with the lookup arguments defaulted to "nothing found"."""
    return _aggregate(
        evidence,
        _NAMES,
        target_map or {},
        _NO_LITERATURE if lit is None else lit,
    )


class TestBioactivityConceptMap:
    def test_maps_concept_to_entity_id(self):
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "b1",
                    "external_ids": {"bioactivity_concept": ["E300003"]},
                }
            ]
        )
        assert bioactivity_concept_map(entities) == {"E300003": "b1"}

    def test_handles_multiple_concepts_per_entity(self):
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "b1",
                    "external_ids": {"bioactivity_concept": ["E1", "E2"]},
                }
            ]
        )
        assert bioactivity_concept_map(entities) == {"E1": "b1", "E2": "b1"}

    def test_ignores_non_dict_external_ids(self):
        entities = pd.DataFrame([{"foodatlas_id": "b1", "external_ids": None}])
        assert bioactivity_concept_map(entities) == {}


class TestRelationshipsPerRow:
    def test_collects_distinct_sorted(self):
        evidence = _evidence(
            [
                {
                    "disease_id": "d1",
                    "bioactivity_id": "b1",
                    "chemical_id": "c1",
                    "relationship": ["therapeutic", "marker/mechanism"],
                },
                {
                    "disease_id": "d1",
                    "bioactivity_id": "b1",
                    "chemical_id": "c1",
                    "relationship": ["therapeutic"],
                },
            ]
        )
        s = _relationships_per_row(
            evidence, ["disease_id", "bioactivity_id", "chemical_id"]
        )
        assert s.iloc[0] == ["marker/mechanism", "therapeutic"]


class TestAggregate:
    def test_counts_distinct_assays_and_measurements(self):
        evidence = _evidence(
            [
                {
                    "disease_id": "d1",
                    "bioactivity_id": "b1",
                    "chemical_id": "c1",
                    "source_assay_id": "AID: 1",
                    "bm": "bm1",
                    "relationship": ["therapeutic"],
                },
                # Same assay, second measurement — one assay, two measurements.
                {
                    "disease_id": "d1",
                    "bioactivity_id": "b1",
                    "chemical_id": "c1",
                    "source_assay_id": "AID: 1",
                    "bm": "bm2",
                    "relationship": ["therapeutic"],
                },
                {
                    "disease_id": "d1",
                    "bioactivity_id": "b1",
                    "chemical_id": "c1",
                    "source_assay_id": "AID: 2",
                    "bm": "bm3",
                    "relationship": ["therapeutic"],
                },
            ]
        )
        out = _agg(evidence)
        assert len(out) == 1
        assert out.iloc[0]["n_assays"] == 2
        assert out.iloc[0]["n_active_measurements"] == 3
        assert out.iloc[0]["disease_name"] == "melanoma"
        assert out.iloc[0]["bioactivity_name"] == "anticancer"
        assert out.iloc[0]["chemical_name"] == "quercetin"

    def test_splits_rows_per_bioactivity(self):
        """One assay classified under two bioactivities yields two rows.

        This is the behaviour the whole view exists for: the disease is
        credited to each activity separately, not to the chemical wholesale.
        """
        evidence = _evidence(
            [
                {
                    "disease_id": "d1",
                    "bioactivity_id": bio,
                    "chemical_id": "c1",
                    "source_assay_id": "AID: 1",
                    "bm": "bm1",
                    "relationship": ["therapeutic"],
                }
                for bio in ("b1", "b2")
            ]
        )
        out = _agg(evidence)
        assert set(out["bioactivity_name"]) == {"anticancer", "antiviral"}

    def test_drops_rows_with_unresolvable_names(self):
        evidence = _evidence(
            [
                {
                    "disease_id": "d1",
                    "bioactivity_id": "unknown",
                    "chemical_id": "c1",
                    "source_assay_id": "AID: 1",
                    "bm": "bm1",
                    "relationship": ["therapeutic"],
                }
            ]
        )
        assert _agg(evidence).empty

    def test_missing_relationship_becomes_empty_list(self):
        evidence = _evidence(
            [
                {
                    "disease_id": "d1",
                    "bioactivity_id": "b1",
                    "chemical_id": "c1",
                    "source_assay_id": "AID: 1",
                    "bm": "bm1",
                    "relationship": None,
                }
            ]
        )
        out = _agg(evidence)
        assert out.iloc[0]["relationships"] == []


class TestMaterializeSkips:
    """Each missing input short-circuits instead of writing a partial view."""

    @patch("src.etl.materializer_disease_bioactivity.build_bridge_evidence")
    def test_skips_when_no_evidence(self, mock_evidence):
        mock_evidence.return_value = (pd.DataFrame(), {})
        with patch("src.etl.materializer_disease_bioactivity.bulk_copy") as mock_copy:
            materialize_disease_bioactivity(MagicMock())
        mock_copy.assert_not_called()

    @patch("src.etl.materializer_disease_bioactivity.assay_bioactivity_map")
    @patch("src.etl.materializer_disease_bioactivity.build_bridge_evidence")
    def test_skips_when_no_assay_classifications(self, mock_evidence, mock_map):
        mock_evidence.return_value = (
            _evidence([{"source_assay_id": "AID: 1", "disease_id": "d1"}]),
            _NAMES,
        )
        mock_map.return_value = pd.DataFrame()
        with patch("src.etl.materializer_disease_bioactivity.bulk_copy") as mock_copy:
            materialize_disease_bioactivity(MagicMock())
        mock_copy.assert_not_called()

    @patch("src.etl.materializer_disease_bioactivity.assay_bioactivity_map")
    @patch("src.etl.materializer_disease_bioactivity.build_bridge_evidence")
    def test_skips_when_no_bridging_assay_is_classified(self, mock_evidence, mock_map):
        """Bridge and classifications both exist but cover disjoint assays."""
        mock_evidence.return_value = (
            _evidence([{"source_assay_id": "AID: 1", "disease_id": "d1"}]),
            _NAMES,
        )
        mock_map.return_value = pd.DataFrame(
            [{"source_assay_id": "AID: 999", "bioactivity_id": "b1"}]
        )
        with patch("src.etl.materializer_disease_bioactivity.bulk_copy") as mock_copy:
            materialize_disease_bioactivity(MagicMock())
        mock_copy.assert_not_called()

    @patch("src.etl.materializer_disease_bioactivity.literature_directions")
    @patch("src.etl.materializer_disease_bioactivity.target_gene_map")
    @patch("src.etl.materializer_disease_bioactivity.assay_bioactivity_map")
    @patch("src.etl.materializer_disease_bioactivity.build_bridge_evidence")
    def test_writes_rows_when_inputs_line_up(
        self, mock_evidence, mock_map, mock_genes, mock_lit
    ):
        mock_evidence.return_value = (
            _evidence(
                [
                    {
                        "source_assay_id": "AID: 1",
                        "disease_id": "d1",
                        "chemical_id": "c1",
                        "bm": "bm1",
                        "relationship": ["therapeutic"],
                        "bioactivity_disease_metadata_id": ["bdm1"],
                    }
                ]
            ),
            _NAMES,
        )
        mock_map.return_value = pd.DataFrame(
            [{"source_assay_id": "AID: 1", "bioactivity_id": "b1"}]
        )
        mock_genes.return_value = {"bdm1": ["NCBIGene: 4780"]}
        mock_lit.return_value = pd.DataFrame(
            [
                {
                    "chemical_id": "c1",
                    "disease_id": "d1",
                    "literature_directions": ["therapeutic"],
                }
            ]
        )
        with patch("src.etl.materializer_disease_bioactivity.bulk_copy") as mock_copy:
            materialize_disease_bioactivity(MagicMock())
        assert mock_copy.call_count == 1
        written = mock_copy.call_args[0][2]
        assert written.iloc[0]["bioactivity_name"] == "anticancer"
        # The evidence arrays the disease side previously had to do without.
        assert written.iloc[0]["target_genes"] == ["NCBIGene: 4780"]
        assert written.iloc[0]["assays"] == ["AID: 1"]
        assert written.iloc[0]["literature_directions"] == ["therapeutic"]
