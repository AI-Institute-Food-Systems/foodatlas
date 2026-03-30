"""Tests for adapt triplet builders."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from src.construct.triplets.chemical_ontology import create_chemical_ontology
from src.construct.triplets.food_ontology import create_food_ontology
from src.models.settings import KGCSettings
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_CHEMICAL_ONTOLOGY,
    FILE_ENTITIES,
    FILE_FOOD_ONTOLOGY,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)
from src.utils.json_io import write_json

if TYPE_CHECKING:
    from pathlib import Path


def _make_store(tmp_path: Path, entities: list[dict]) -> EntityStore:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir(exist_ok=True)
    write_json(kg_dir / FILE_ENTITIES, entities)
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
                "synonyms_display": {},
            },
            {
                "foodatlas_id": "e2",
                "entity_type": "food",
                "common_name": "fruit",
                "synonyms": ["fruit"],
                "external_ids": {"foodon": ["F2"]},
                "scientific_name": "",
                "synonyms_display": {},
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
    settings = KGCSettings(kg_dir=str(tmp_path / "kg"))
    result = create_food_ontology(store, sources, settings)
    assert len(result) == 1
    assert result.iloc[0]["head_id"] == "e1"
    assert result.iloc[0]["tail_id"] == "e2"
    assert (tmp_path / "kg" / FILE_FOOD_ONTOLOGY).exists()


def test_food_ontology_no_foodon() -> None:
    result = create_food_ontology(
        EntityStore.__new__(EntityStore),
        {},
        KGCSettings.__new__(KGCSettings),
    )
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
                "synonyms_display": {},
            },
            {
                "foodatlas_id": "e2",
                "entity_type": "chemical",
                "common_name": "molecule",
                "synonyms": ["molecule"],
                "external_ids": {"chebi": [200]},
                "scientific_name": "",
                "synonyms_display": {},
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
    settings = KGCSettings(kg_dir=str(tmp_path / "kg"))
    result = create_chemical_ontology(store, sources, settings)
    assert len(result) == 1
    assert (tmp_path / "kg" / FILE_CHEMICAL_ONTOLOGY).exists()


def test_chemical_ontology_no_chebi() -> None:
    result = create_chemical_ontology(
        EntityStore.__new__(EntityStore),
        {},
        KGCSettings.__new__(KGCSettings),
    )
    assert result.empty
