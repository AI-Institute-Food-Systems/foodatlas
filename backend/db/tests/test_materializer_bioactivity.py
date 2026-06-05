"""Tests for src.etl.materializer_bioactivity — 4 bioactivity MVs."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.materializer_bioactivity import (
    materialize_bioactivity_disease_association,
    materialize_bioactivity_entities,
    materialize_chemical_bioactivity_measurement,
    materialize_food_bioactivity_exhibits,
    refresh_bioactivity,
)

_ATTESTATION_COLUMNS = [
    "attestation_id",
    "evidence_id",
    "bioactivity_metadata_id",
    "source_assay_id",
    "target_ids",
    "evidence_value_potency_value",
    "evidence_value_potency_unit",
    "evidence_value_efficacy_zeroactivity",
    "evidence_value_efficacy_infiniteactivity",
    "evidence_value_efficacy_logac50_value",
    "evidence_value_efficacy_hillslope",
    "evidence_source",
    "evidence_type",
    "exhibit_type",
    "polarity",
    "derived_from_attestation_id",
    "via_chemical_id",
]


def _att(
    attestation_id: str,
    bioactivity_metadata_id: str,
    **overrides: object,
) -> dict:
    """Return a default attestation dict with selective overrides."""
    base = dict.fromkeys(_ATTESTATION_COLUMNS)
    base["attestation_id"] = attestation_id
    base["evidence_id"] = "ev1"
    base["bioactivity_metadata_id"] = bioactivity_metadata_id
    base["target_ids"] = []
    base.update(overrides)
    return base


def _stub_read_sql(routes: dict[str, pd.DataFrame]):
    """Return a side_effect that picks a DataFrame by SQL substring match."""

    def fake(query, _conn=None):
        sql = str(query)
        for substr, df in routes.items():
            if substr in sql:
                return df.copy()
        return pd.DataFrame()

    return fake


class TestMaterializeBioactivityEntities:
    @patch("src.etl.materializer_bioactivity.bulk_copy")
    @patch("src.etl.materializer_bioactivity.pd.read_sql")
    def test_filters_to_relevant_bioactivities(self, mock_read_sql, mock_bulk):
        entities = pd.DataFrame(
            {
                "foodatlas_id": ["bio001", "bio002", "bio003"],
                "entity_type": ["bioactivity"] * 3,
                "common_name": ["anti-inflammatory", "antioxidant", "unused"],
                "scientific_name": ["", "", ""],
                "synonyms": [[], [], []],
                "external_ids": [{}, {}, {}],
                "attributes": [
                    {"description": "desc1"},
                    {"description": "desc2"},
                    {"description": "desc3"},
                ],
            }
        )
        triplets = pd.DataFrame(
            {
                "head_id": ["c1", "f1", "bio001"],
                "tail_id": ["bio001", "bio002", "d1"],
                "relationship_id": ["r5", "r6", "r7"],
            }
        )
        mock_read_sql.side_effect = _stub_read_sql(
            {"entity_type = 'bioactivity'": entities, "r5','r6','r7": triplets}
        )

        materialize_bioactivity_entities(MagicMock())

        assert mock_bulk.called
        df_arg = mock_bulk.call_args.args[2]
        ids_inserted = set(df_arg["foodatlas_id"])
        assert ids_inserted == {"bio001", "bio002"}
        descriptions = dict(
            zip(df_arg["foodatlas_id"], df_arg["description"], strict=False)
        )
        assert descriptions["bio001"] == "desc1"

    @patch("src.etl.materializer_bioactivity.bulk_copy")
    @patch("src.etl.materializer_bioactivity.pd.read_sql")
    def test_skips_when_no_bioactivity_entities(self, mock_read_sql, mock_bulk):
        mock_read_sql.side_effect = _stub_read_sql({"entity_type": pd.DataFrame()})
        materialize_bioactivity_entities(MagicMock())
        assert not mock_bulk.called


class TestMaterializeChemicalBioactivityMeasurement:
    @patch("src.etl.materializer_bioactivity.bulk_copy")
    @patch("src.etl.materializer_bioactivity.pd.read_sql")
    def test_aggregates_per_chemical_bioactivity_pair(self, mock_read_sql, mock_bulk):
        triplets = pd.DataFrame(
            {
                "head_id": ["c1"],
                "tail_id": ["bio001"],
                "attestation_ids": [["ba1", "ba2"]],
            }
        )
        atts = pd.DataFrame(
            [
                _att(
                    "ba1",
                    "BAM000001",
                    source_assay_id="A1",
                    evidence_value_potency_value=5.0,
                    evidence_value_potency_unit="uM",
                ),
                _att("ba2", "BAM000002", source_assay_id="A2"),
            ]
        )
        entities = pd.DataFrame(
            {
                "foodatlas_id": ["c1", "bio001"],
                "common_name": ["quercetin", "anti-inflammatory"],
            }
        )
        mock_read_sql.side_effect = _stub_read_sql(
            {
                "relationship_id = 'r5'": triplets,
                "base_bioactivity_attestations": atts,
                "base_entities": entities,
            }
        )

        materialize_chemical_bioactivity_measurement(MagicMock())

        df_arg = mock_bulk.call_args.args[2]
        assert len(df_arg) == 1
        row = df_arg.iloc[0]
        assert row["chemical_foodatlas_id"] == "c1"
        assert row["bioactivity_foodatlas_id"] == "bio001"
        assert row["measurement_count"] == 2
        payload = json.loads(row["measurements"])
        assert {m["attestation_id"] for m in payload} == {"ba1", "ba2"}


class TestMaterializeFoodBioactivityExhibits:
    @patch("src.etl.materializer_bioactivity.bulk_copy")
    @patch("src.etl.materializer_bioactivity.pd.read_sql")
    def test_splits_direct_and_inherited(self, mock_read_sql, mock_bulk):
        triplets = pd.DataFrame(
            {
                "head_id": ["f1"],
                "tail_id": ["bio001"],
                "attestation_ids": [["ba_dir", "ba_inh"]],
            }
        )
        atts = pd.DataFrame(
            [
                _att("ba_dir", "BAM000010", exhibit_type="direct"),
                _att(
                    "ba_inh",
                    "BAM000001",
                    exhibit_type="inherited",
                    derived_from_attestation_id="ba001",
                    via_chemical_id="c1",
                ),
            ]
        )
        entities = pd.DataFrame(
            {
                "foodatlas_id": ["f1", "bio001", "c1"],
                "common_name": ["strawberry", "anti-inflammatory", "quercetin"],
            }
        )
        mock_read_sql.side_effect = _stub_read_sql(
            {
                "relationship_id = 'r6'": triplets,
                "base_bioactivity_attestations": atts,
                "base_entities": entities,
            }
        )

        materialize_food_bioactivity_exhibits(MagicMock())

        df_arg = mock_bulk.call_args.args[2]
        assert len(df_arg) == 2
        by_type = {row["exhibit_type"]: row for _, row in df_arg.iterrows()}
        assert pd.isna(by_type["direct"]["via_chemical_id"])
        assert by_type["inherited"]["via_chemical_id"] == "c1"
        assert by_type["inherited"]["via_chemical_name"] == "quercetin"
        # Efficacy_pred reserved but NULL in v1
        assert pd.isna(by_type["inherited"]["efficacy_pred"])

    @patch("src.etl.materializer_bioactivity.bulk_copy")
    @patch("src.etl.materializer_bioactivity.pd.read_sql")
    def test_null_exhibit_type_treated_as_direct(self, mock_read_sql, mock_bulk):
        triplets = pd.DataFrame(
            {
                "head_id": ["f1"],
                "tail_id": ["bio001"],
                "attestation_ids": [["ba1"]],
            }
        )
        atts = pd.DataFrame([_att("ba1", "BAM000001", exhibit_type=None)])
        entities = pd.DataFrame(
            {"foodatlas_id": ["f1", "bio001"], "common_name": ["strawberry", "a"]}
        )
        mock_read_sql.side_effect = _stub_read_sql(
            {
                "relationship_id = 'r6'": triplets,
                "base_bioactivity_attestations": atts,
                "base_entities": entities,
            }
        )

        materialize_food_bioactivity_exhibits(MagicMock())

        df_arg = mock_bulk.call_args.args[2]
        assert df_arg.iloc[0]["exhibit_type"] == "direct"


class TestMaterializeBioactivityDiseaseAssociation:
    @patch("src.etl.materializer_bioactivity.bulk_copy")
    @patch("src.etl.materializer_bioactivity.pd.read_sql")
    def test_aggregates_targets(self, mock_read_sql, mock_bulk):
        triplets = pd.DataFrame(
            {
                "head_id": ["bio001"],
                "tail_id": ["d1"],
                "attestation_ids": [["ba1", "ba2"]],
            }
        )
        atts = pd.DataFrame(
            [
                _att("ba1", "BDM000001", target_ids=["UniProt:P1", "UniProt:P2"]),
                _att("ba2", "BDM000002", target_ids=["UniProt:P2", "UniProt:P3"]),
            ]
        )
        entities = pd.DataFrame(
            {
                "foodatlas_id": ["bio001", "d1"],
                "common_name": ["anti-inflammatory", "asthma"],
            }
        )
        mock_read_sql.side_effect = _stub_read_sql(
            {
                "relationship_id = 'r7'": triplets,
                "base_bioactivity_attestations": atts,
                "base_entities": entities,
            }
        )

        materialize_bioactivity_disease_association(MagicMock())

        df_arg = mock_bulk.call_args.args[2]
        row = df_arg.iloc[0]
        assert row["bioactivity_foodatlas_id"] == "bio001"
        assert row["disease_foodatlas_id"] == "d1"
        assert sorted(row["target_ids"]) == [
            "UniProt:P1",
            "UniProt:P2",
            "UniProt:P3",
        ]
        assert row["evidence_count"] == 2


class TestRefreshBioactivity:
    @patch(
        "src.etl.materializer_bioactivity.materialize_bioactivity_disease_association"
    )
    @patch("src.etl.materializer_bioactivity.materialize_food_bioactivity_exhibits")
    @patch(
        "src.etl.materializer_bioactivity.materialize_chemical_bioactivity_measurement"
    )
    @patch("src.etl.materializer_bioactivity.materialize_bioactivity_entities")
    def test_calls_all_four(self, mock_ent, mock_chem, mock_food, mock_dis):
        conn = MagicMock()
        refresh_bioactivity(conn)
        mock_ent.assert_called_once_with(conn)
        mock_chem.assert_called_once_with(conn)
        mock_food.assert_called_once_with(conn)
        mock_dis.assert_called_once_with(conn)
