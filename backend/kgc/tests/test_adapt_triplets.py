"""Tests for adapt triplet builders."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
from src.pipeline.triplets.chemical_ontology import create_chemical_ontology
from src.pipeline.triplets.food_ontology import create_food_ontology
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)
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


def test_food_ontology_from_edges(tmp_path: Path) -> None:
    store = _make_store(
        tmp_path,
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
    result = create_food_ontology(store, sources)
    assert len(result) == 1
    assert result.iloc[0]["head_id"] == "e1"
    assert result.iloc[0]["tail_id"] == "e2"
    assert result.iloc[0]["source"] == "foodon"


def test_food_ontology_no_foodon() -> None:
    result = create_food_ontology(EntityStore.__new__(EntityStore), {})
    assert result.empty


def test_chemical_ontology_from_edges(tmp_path: Path) -> None:
    store = _make_store(
        tmp_path,
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
    result = create_chemical_ontology(store, sources)
    assert len(result) == 1
    assert result.iloc[0]["source"] == "chebi"


def test_chemical_ontology_no_chebi() -> None:
    result = create_chemical_ontology(EntityStore.__new__(EntityStore), {})
    assert result.empty
