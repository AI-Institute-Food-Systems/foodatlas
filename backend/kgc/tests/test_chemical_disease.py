"""Tests for chemical-disease and disease ontology triplet builders."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pandas as pd
from src.pipeline.triplets.chemical_disease import merge_ctd_triplets
from src.pipeline.triplets.disease_disease import create_disease_ontology
from src.stores.entity_store import EntityStore
from src.stores.schema import FILE_ENTITIES, FILE_LUT_CHEMICAL, FILE_LUT_FOOD
from src.utils.json_io import write_json

if TYPE_CHECKING:
    from pathlib import Path


def _make_store(tmp_path: Path, entities: list[dict]) -> EntityStore:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir(exist_ok=True)
    df = pd.DataFrame(entities)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(json.dumps)
    df.to_parquet(kg_dir / FILE_ENTITIES, index=False)
    write_json(kg_dir / FILE_LUT_FOOD, {})
    write_json(kg_dir / FILE_LUT_CHEMICAL, {})
    return EntityStore(
        path_entities=kg_dir / FILE_ENTITIES,
        path_lut_food=kg_dir / FILE_LUT_FOOD,
        path_lut_chemical=kg_dir / FILE_LUT_CHEMICAL,
    )


class TestMergeCtdTriplets:
    def test_creates_triplets(self, tmp_path: Path) -> None:
        store = _make_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "chemical",
                    "common_name": "aspirin",
                    "synonyms": ["aspirin"],
                    "external_ids": {"chebi": [15365], "mesh": ["D001241"]},
                    "scientific_name": "",
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "disease",
                    "common_name": "asthma",
                    "synonyms": ["asthma"],
                    "external_ids": {"ctd": ["MESH:D001249"]},
                    "scientific_name": "",
                },
            ],
        )
        kg = MagicMock()
        kg.entities = store
        kg.triplets = MagicMock()

        sources = {
            "ctd": {
                "edges": pd.DataFrame(
                    [
                        {
                            "source_id": "ctd",
                            "head_native_id": "D001241",
                            "tail_native_id": "MESH:D001249",
                            "edge_type": "chemical_disease_association",
                            "raw_attrs": {"direct_evidence": "therapeutic"},
                        },
                        {
                            "source_id": "ctd",
                            "head_native_id": "D001241",
                            "tail_native_id": "MESH:D001249",
                            "edge_type": "chemical_disease_association",
                            "raw_attrs": {"direct_evidence": "marker/mechanism"},
                        },
                    ]
                ),
            }
        }
        merge_ctd_triplets(kg, sources)
        call_args = kg.triplets.add_ontology.call_args[0][0]
        assert len(call_args) == 2
        assert set(call_args["relationship_id"]) == {"r3", "r4"}

    def test_skips_unresolvable(self, tmp_path: Path) -> None:
        store = _make_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "chemical",
                    "common_name": "aspirin",
                    "synonyms": ["aspirin"],
                    "external_ids": {"chebi": [15365], "mesh": ["D001241"]},
                    "scientific_name": "",
                },
            ],
        )
        kg = MagicMock()
        kg.entities = store
        kg.triplets = MagicMock()

        sources = {
            "ctd": {
                "edges": pd.DataFrame(
                    [
                        {
                            "source_id": "ctd",
                            "head_native_id": "D001241",
                            "tail_native_id": "MESH:D999999",
                            "edge_type": "chemical_disease_association",
                            "raw_attrs": {"direct_evidence": "therapeutic"},
                        },
                    ]
                ),
            }
        }
        merge_ctd_triplets(kg, sources)
        call_args = kg.triplets.add_ontology.call_args[0][0]
        assert len(call_args) == 0

    def test_no_ctd_source(self) -> None:
        kg = MagicMock()
        merge_ctd_triplets(kg, {})
        kg.triplets.add_ontology.assert_not_called()


class TestDiseaseOntology:
    def test_creates_is_a_triplets(self, tmp_path: Path) -> None:
        store = _make_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "disease",
                    "common_name": "asthma",
                    "synonyms": ["asthma"],
                    "external_ids": {"ctd": ["MESH:D001249"]},
                    "scientific_name": "",
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "disease",
                    "common_name": "respiratory disease",
                    "synonyms": ["respiratory disease"],
                    "external_ids": {"ctd": ["MESH:D012140"]},
                    "scientific_name": "",
                },
            ],
        )
        sources = {
            "ctd": {
                "edges": pd.DataFrame(
                    [
                        {
                            "source_id": "ctd",
                            "head_native_id": "MESH:D001249",
                            "tail_native_id": "MESH:D012140",
                            "edge_type": "is_a",
                            "raw_attrs": {},
                        },
                    ]
                ),
            }
        }
        result = create_disease_ontology(store, sources)
        assert len(result) == 1
        assert result.iloc[0]["head_id"] == "e1"
        assert result.iloc[0]["tail_id"] == "e2"
        assert result.iloc[0]["source"] == "ctd"

    def test_no_ctd_source(self) -> None:
        result = create_disease_ontology(EntityStore.__new__(EntityStore), {})
        assert result.empty
