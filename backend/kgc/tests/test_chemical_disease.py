"""Tests for chemical-disease and disease ontology triplet builders."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
from src.models.settings import KGCSettings
from src.pipeline.knowledge_graph import KnowledgeGraph
from src.pipeline.triplets.chemical_disease import merge_ctd_triplets
from src.pipeline.triplets.disease_disease import merge_disease_ontology
from src.stores.schema import (
    FILE_ATTESTATIONS,
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_TRIPLETS,
)

if TYPE_CHECKING:
    from pathlib import Path


def _make_kg(tmp_path: Path, entities: list[dict]) -> KnowledgeGraph:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir(exist_ok=True)
    df = pd.DataFrame(entities)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(json.dumps)
    df.to_parquet(kg_dir / FILE_ENTITIES, index=False)
    pd.DataFrame().to_parquet(kg_dir / FILE_TRIPLETS)
    pd.DataFrame().to_parquet(kg_dir / FILE_EVIDENCE)
    pd.DataFrame().to_parquet(kg_dir / FILE_ATTESTATIONS)
    for f in (FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
        (kg_dir / f).parent.mkdir(parents=True, exist_ok=True)
        (kg_dir / f).write_text("{}")
    settings = KGCSettings(kg_dir=str(kg_dir), cache_dir=str(kg_dir / "_cache"))
    return KnowledgeGraph(settings)


class TestMergeCtdTriplets:
    def test_creates_triplets(self, tmp_path: Path) -> None:
        kg = _make_kg(
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
        sources = {
            "ctd": {
                "edges": pd.DataFrame(
                    [
                        {
                            "source_id": "ctd",
                            "head_native_id": "D001241",
                            "tail_native_id": "MESH:D001249",
                            "edge_type": "chemical_disease_association",
                            "raw_attrs": {
                                "direct_evidence": "therapeutic",
                                "PubMedIDs": [12345],
                            },
                        },
                    ]
                ),
            }
        }
        merge_ctd_triplets(kg, sources)
        assert len(kg.triplets._triplets) == 1

    def test_skips_unresolvable(self, tmp_path: Path) -> None:
        kg = _make_kg(
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
        sources = {
            "ctd": {
                "edges": pd.DataFrame(
                    [
                        {
                            "source_id": "ctd",
                            "head_native_id": "D001241",
                            "tail_native_id": "MESH:D999999",
                            "edge_type": "chemical_disease_association",
                            "raw_attrs": {
                                "direct_evidence": "therapeutic",
                                "PubMedIDs": [99999],
                            },
                        },
                    ]
                ),
            }
        }
        merge_ctd_triplets(kg, sources)
        assert len(kg.triplets._triplets) == 0

    def test_no_ctd_source(self, tmp_path: Path) -> None:
        kg = _make_kg(tmp_path, [])
        merge_ctd_triplets(kg, {})
        assert len(kg.triplets._triplets) == 0


class TestDiseaseOntology:
    def test_creates_is_a_triplets(self, tmp_path: Path) -> None:
        kg = _make_kg(
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
        merge_disease_ontology(kg, sources)
        assert len(kg.triplets._triplets) == 1

    def test_no_ctd_source(self, tmp_path: Path) -> None:
        kg = _make_kg(tmp_path, [])
        merge_disease_ontology(kg, {})
        assert len(kg.triplets._triplets) == 0
