"""Tests for the TripletStore class."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.stores.schema import FILE_TRIPLETS
from src.stores.triplet_store import TripletStore


@pytest.fixture()
def triplets_dir(tmp_path: Path) -> Path:
    data = [
        {
            "head_id": "e0",
            "relationship_id": "r1",
            "tail_id": "e1",
            "source": "fdc",
            "attestation_ids": json.dumps(["mc0"]),
        },
    ]
    pd.DataFrame(data).to_parquet(tmp_path / FILE_TRIPLETS, index=False)
    return tmp_path


@pytest.fixture()
def triplets(triplets_dir: Path) -> TripletStore:
    return TripletStore(path_triplets=triplets_dir / FILE_TRIPLETS)


class TestTripletStoreLoad:
    def test_loads_triplets(self, triplets: TripletStore) -> None:
        assert len(triplets._triplets) == 1

    def test_composite_key_index(self, triplets: TripletStore) -> None:
        assert "e0_r1_e1" in triplets._triplets.index

    def test_hash_table_built(self, triplets: TripletStore) -> None:
        assert "e0_r1_e1" in triplets._key_to_attestations


class TestTripletStoreCreate:
    def test_create_new_triplet(self, triplets: TripletStore) -> None:
        metadata = pd.DataFrame(
            [
                {
                    "head_id": "e2",
                    "relationship_id": "r1",
                    "tail_id": "e3",
                }
            ],
            index=pd.Index(["mc1"], name="foodatlas_id"),
        )
        triplets.create(metadata)
        assert len(triplets._triplets) == 2
        assert "e2_r1_e3" in triplets._key_to_attestations

    def test_dedup_merges_metadata(self, triplets: TripletStore) -> None:
        metadata = pd.DataFrame(
            [
                {
                    "head_id": "e0",
                    "relationship_id": "r1",
                    "tail_id": "e1",
                }
            ],
            index=pd.Index(["mc1"], name="foodatlas_id"),
        )
        triplets.create(metadata)
        assert len(triplets._triplets) == 1
        row = triplets._triplets.loc["e0_r1_e1"]
        assert "mc0" in row["attestation_ids"]
        assert "mc1" in row["attestation_ids"]

    def test_composite_key_used(self, triplets: TripletStore) -> None:
        metadata = pd.DataFrame(
            [
                {"head_id": "e10", "relationship_id": "r1", "tail_id": "e11"},
                {"head_id": "e12", "relationship_id": "r1", "tail_id": "e13"},
            ],
            index=pd.Index(["mc10", "mc11"], name="foodatlas_id"),
        )
        triplets.create(metadata)
        assert "e10_r1_e11" in triplets._triplets.index
        assert "e12_r1_e13" in triplets._triplets.index


class TestTripletStoreGetByRelationship:
    def test_filters_by_relationship(self, triplets: TripletStore) -> None:
        result = triplets.get_by_relationship_id("r1")
        assert len(result) == 1

    def test_empty_for_unknown_relationship(self, triplets: TripletStore) -> None:
        result = triplets.get_by_relationship_id("r999")
        assert len(result) == 0


class TestTripletStoreAddOntology:
    def test_add_ontology_triplets(self, triplets: TripletStore) -> None:
        onto = pd.DataFrame(
            [
                {
                    "head_id": "e10",
                    "relationship_id": "r2",
                    "tail_id": "e20",
                    "source": "foodon",
                }
            ]
        )
        triplets.add_ontology(onto)
        assert len(triplets._triplets) == 2
        assert "e10_r2_e20" in triplets._key_to_attestations

    def test_add_empty_ontology(self, triplets: TripletStore) -> None:
        triplets.add_ontology(pd.DataFrame())
        assert len(triplets._triplets) == 1


class TestTripletStoreSaveReload:
    def test_round_trip(self, triplets: TripletStore, tmp_path: Path) -> None:
        out_dir = tmp_path / "output"
        out_dir.mkdir()
        triplets.save(out_dir)

        reloaded = TripletStore(path_triplets=out_dir / FILE_TRIPLETS)
        assert len(reloaded._triplets) == len(triplets._triplets)
        assert "e0_r1_e1" in reloaded._key_to_attestations
