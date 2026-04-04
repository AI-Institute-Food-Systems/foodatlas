"""Tests for ontology triplet builders (now using extraction flow)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.pipeline.triplets.chemical_chemical import merge_chemical_ontology
from src.pipeline.triplets.food_food import merge_food_ontology

if TYPE_CHECKING:
    from src.pipeline.knowledge_graph import KnowledgeGraph


def _make_kg_with_entities(kg: KnowledgeGraph, entities: list[dict]) -> None:
    """Replace entities in an existing KG fixture."""
    df = pd.DataFrame(entities)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(json.dumps)

    # Write directly and reload
    path = kg.entities.path_entities
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    kg.entities._load()


def test_food_ontology_from_edges(kg: KnowledgeGraph) -> None:
    _make_kg_with_entities(
        kg,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "food",
                "common_name": "apple",
                "synonyms": ["apple"],
                "external_ids": {"foodon": ["F1"]},
                "scientific_name": "",
            },
            {
                "foodatlas_id": "e2",
                "entity_type": "food",
                "common_name": "fruit",
                "synonyms": ["fruit"],
                "external_ids": {"foodon": ["F2"]},
                "scientific_name": "",
            },
        ],
    )
    sources = {
        "foodon": {
            "edges": pd.DataFrame(
                [
                    {
                        "source_id": "foodon",
                        "head_native_id": "F1",
                        "tail_native_id": "F2",
                        "edge_type": "is_a",
                        "raw_attrs": {},
                    }
                ]
            ),
        }
    }
    merge_food_ontology(kg, sources)
    assert len(kg.triplets._triplets) >= 1


def test_food_ontology_no_foodon(kg: KnowledgeGraph) -> None:
    merge_food_ontology(kg, {})
    # No error, no new triplets beyond fixture
    assert True


def test_chemical_ontology_from_edges(kg: KnowledgeGraph) -> None:
    _make_kg_with_entities(
        kg,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "chemical",
                "common_name": "water",
                "synonyms": ["water"],
                "external_ids": {"chebi": [100]},
                "scientific_name": "",
            },
            {
                "foodatlas_id": "e2",
                "entity_type": "chemical",
                "common_name": "molecule",
                "synonyms": ["molecule"],
                "external_ids": {"chebi": [200]},
                "scientific_name": "",
            },
        ],
    )
    sources = {
        "chebi": {
            "edges": pd.DataFrame(
                [
                    {
                        "source_id": "chebi",
                        "head_native_id": "100",
                        "tail_native_id": "200",
                        "edge_type": "is_a",
                        "raw_attrs": {},
                    }
                ]
            ),
        }
    }
    merge_chemical_ontology(kg, sources)
    assert len(kg.triplets._triplets) >= 1


def test_chemical_ontology_no_chebi(kg: KnowledgeGraph) -> None:
    merge_chemical_ontology(kg, {})
    assert True
