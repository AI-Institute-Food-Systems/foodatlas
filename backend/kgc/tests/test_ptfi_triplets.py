"""Tests for the PTFI triplets builder — attestation shape, per-sample
granularity, and narrowing an ambiguous PubChem CID by the name PTFI gave."""

from __future__ import annotations

import json

import pandas as pd
from src.pipeline.triplets.food_chemical.ptfi import (
    _attestation_rows,
    _prefer_named_chemical,
    _resolve_entities,
    merge_ptfi_triplets,
)

_ENTITIES = pd.DataFrame(
    [
        {
            "foodatlas_id": "e10",
            "entity_type": "food",
            "common_name": "pepper",
            "external_ids": {"ptfi": ["GGB100", "GGB101"]},
        },
        {
            "foodatlas_id": "e20",
            "entity_type": "chemical",
            "common_name": "formate",
            "external_ids": {"pubchem_compound": [283]},
        },
        {
            "foodatlas_id": "e21",
            "entity_type": "chemical",
            "common_name": "monocarboxylic acid anion",
            "external_ids": {"pubchem_compound": [283]},
        },
        {
            "foodatlas_id": "e30",
            "entity_type": "chemical",
            "common_name": "quercitrin",
            "external_ids": {"pubchem_compound": [5192]},
        },
    ]
).set_index("foodatlas_id")


def _edge(sample_id: str, cid: str, name: str, value: float) -> dict:
    return {
        "source_id": "ptfi",
        "head_native_id": "PTFI_FOOD:GGB100",
        "tail_native_id": f"PTFI_CHEM:{cid}",
        "edge_type": "contains",
        "raw_attrs": {
            "sample_id": sample_id,
            "chemical_name": name,
            "conc_value": value,
            "conc_unit": "relative_abundance",
            "dataset": "PTFI vX",
            "platform": "LC-MS",
        },
    }


class _Entities:
    def __init__(self, entities: pd.DataFrame) -> None:
        self._entities = entities


class _Evidence:
    def __init__(self) -> None:
        self.created: pd.DataFrame | None = None

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        self.created = rows
        out = rows.copy()
        out.index = pd.Index(["ev_ptfi"] * len(rows), name="evidence_id")
        return out


class _Attestations:
    def __init__(self) -> None:
        self.rows: pd.DataFrame | None = None

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        self.rows = rows
        out = rows.copy()
        out.index = pd.Index([f"at{i}" for i in range(len(rows))])
        return out


class _Triplets:
    def __init__(self) -> None:
        self.rows: pd.DataFrame | None = None

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        self.rows = rows
        return rows.drop_duplicates(subset=["head_id", "tail_id"])


class _KG:
    def __init__(self, entities: pd.DataFrame = _ENTITIES) -> None:
        self.entities = _Entities(entities)
        self.evidence = _Evidence()
        self.attestations = _Attestations()
        self.triplets = _Triplets()


def _sources(edges: list[dict]) -> dict[str, dict[str, pd.DataFrame]]:
    return {"ptfi": {"edges": pd.DataFrame(edges)}}


class TestAmbiguityNarrowing:
    def test_ambiguous_cid_narrows_to_the_chemical_ptfi_named(self) -> None:
        """CID 283 is both `formate` and a ChEBI class; PTFI said formate."""
        kg = _KG()
        resolved = _resolve_entities(
            kg, pd.DataFrame([_edge("GGB100", "283", "formate", 1.0)])
        )
        assert list(resolved["_tail_id"]) == ["e20"]

    def test_name_match_is_case_insensitive(self) -> None:
        kg = _KG()
        resolved = _resolve_entities(
            kg, pd.DataFrame([_edge("GGB100", "283", "FORMATE", 1.0)])
        )
        assert list(resolved["_tail_id"]) == ["e20"]

    def test_unmatched_name_keeps_every_candidate(self) -> None:
        """Both entities are the same compound; PTFI cannot settle it, so keep both."""
        kg = _KG()
        resolved = _resolve_entities(
            kg, pd.DataFrame([_edge("GGB100", "283", "methanoate", 1.0)])
        )
        assert set(resolved["_tail_id"]) == {"e20", "e21"}

    def test_unambiguous_cid_is_untouched(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "_tail_id": "e30",
                    "tail_candidates": ["e30"],
                    "_sample_id": "GGB100",
                    "_cid": "5192",
                    "raw_attrs": {"chemical_name": "quercitrin"},
                }
            ]
        )
        assert _prefer_named_chemical(df, _ENTITIES).equals(df)


class TestAttestations:
    def test_one_attestation_per_sample_not_per_food(self) -> None:
        """Two samples of one food must stay two measurements, not collapse."""
        kg = _KG()
        edges = [
            _edge("GGB100", "5192", "quercitrin", 12.5),
            _edge("GGB101", "5192", "quercitrin", 30.0),
        ]
        merge_ptfi_triplets(kg, _sources(edges))
        assert len(kg.attestations.rows) == 2
        assert sorted(kg.attestations.rows["head_name_raw"]) == ["GGB100", "GGB101"]
        assert sorted(kg.attestations.rows["conc_value"]) == [12.5, 30.0]

    def test_concentration_passes_through_unconverted(self) -> None:
        kg = _KG()
        rows = _attestation_rows(
            kg,
            _resolve_entities(
                kg, pd.DataFrame([_edge("GGB100", "5192", "quercitrin", 12.5)])
            ),
        )
        assert rows.iloc[0]["conc_unit"] == "relative_abundance"
        assert rows.iloc[0]["conc_value"] == 12.5
        assert rows.iloc[0]["conc_value_raw"] == "12.5"
        assert rows.iloc[0]["source"] == "ptfi"
        assert bool(rows.iloc[0]["validated"]) is True

    def test_evidence_records_the_dataset_and_platform(self) -> None:
        kg = _KG()
        _attestation_rows(
            kg,
            _resolve_entities(
                kg, pd.DataFrame([_edge("GGB100", "5192", "quercitrin", 1.0)])
            ),
        )
        reference = json.loads(kg.evidence.created.iloc[0]["reference"])
        assert reference == {
            "source": "ptfi",
            "dataset": "PTFI vX",
            "platform": "LC-MS",
            "measure": "relative_abundance",
        }


class TestTriplets:
    def test_samples_of_one_food_collapse_to_one_triplet(self) -> None:
        kg = _KG()
        edges = [
            _edge("GGB100", "5192", "quercitrin", 12.5),
            _edge("GGB101", "5192", "quercitrin", 30.0),
        ]
        merge_ptfi_triplets(kg, _sources(edges))
        assert len(kg.triplets.rows.drop_duplicates(["head_id", "tail_id"])) == 1
        assert set(kg.triplets.rows["relationship_id"]) == {"r1"}


class TestNoData:
    def test_absent_source_is_a_no_op(self) -> None:
        kg = _KG()
        merge_ptfi_triplets(kg, {})
        assert kg.attestations.rows is None

    def test_no_contains_edges_is_a_no_op(self) -> None:
        kg = _KG()
        merge_ptfi_triplets(kg, _sources([]))
        assert kg.attestations.rows is None

    def test_unresolvable_sample_yields_nothing(self) -> None:
        kg = _KG()
        merge_ptfi_triplets(kg, _sources([_edge("GGB_UNKNOWN", "5192", "x", 1.0)]))
        assert kg.attestations.rows is None
