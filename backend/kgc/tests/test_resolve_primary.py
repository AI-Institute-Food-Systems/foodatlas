"""Tests for Pass 1 entity resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.config.corrections import ChebiLutCorrections, Corrections
from src.pipeline.entities.resolve_primary import (
    create_chemicals_from_chebi,
    create_diseases_from_ctd,
    create_foods_from_foodon,
    pick_common_name,
)
from src.pipeline.entities.utils.lut import EntityLUT
from src.stores.entity_registry import EntityRegistry
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    REGISTRY_COLUMNS,
)
from src.utils.json_io import write_json

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture()
def registry(tmp_path: Path) -> EntityRegistry:
    path = tmp_path / "entity_registry.parquet"
    pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(path, index=False)
    return EntityRegistry(path)


def _make_store(tmp_path: Path) -> EntityStore:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir()
    pd.DataFrame().to_parquet(kg_dir / FILE_ENTITIES)
    write_json(kg_dir / FILE_LUT_FOOD, {})
    write_json(kg_dir / FILE_LUT_CHEMICAL, {})
    return EntityStore(
        path_entities=kg_dir / FILE_ENTITIES,
        path_lut_food=kg_dir / FILE_LUT_FOOD,
        path_lut_chemical=kg_dir / FILE_LUT_CHEMICAL,
    )


def test_pick_common_name_prefers_label() -> None:
    assert (
        pick_common_name(["taxon_name", "label_name"], ["taxon", "label"])
        == "label_name"
    )


def test_pick_common_name_prefers_name_type() -> None:
    assert pick_common_name(["a_label", "a_name"], ["label", "name"]) == "a_name"


def test_pick_common_name_star_boost() -> None:
    result = pick_common_name(["low", "high"], ["synonym", "name"], star=5)
    assert result == "high"


def test_pick_common_name_empty() -> None:
    assert pick_common_name([], []) == ""


def test_create_foods_from_foodon(tmp_path: Path, registry: EntityRegistry) -> None:
    store = _make_store(tmp_path)
    lut = EntityLUT()
    sources = {
        "foodon": {
            "nodes": pd.DataFrame(
                [
                    {
                        "source_id": "foodon",
                        "native_id": "http://foodon/FOOD_001",
                        "name": "apple",
                        "synonyms": ["apple", "apples"],
                        "synonym_types": ["label", "broad"],
                        "node_type": "class",
                        "raw_attrs": {},
                        "is_food": True,
                    },
                ]
            ),
        }
    }
    create_foods_from_foodon(sources, store, lut, registry)
    assert len(store._entities) == 1
    row = store._entities.iloc[0]
    assert row["entity_type"] == "food"
    assert row["common_name"] == "apple"
    assert lut.lookup("food", "apple") == ["e1"]
    assert lut.lookup("food", "apples") == ["e1"]


def test_create_chemicals_from_chebi(tmp_path: Path, registry: EntityRegistry) -> None:
    store = _make_store(tmp_path)
    lut = EntityLUT()
    sources = {
        "chebi": {
            "nodes": pd.DataFrame(
                [
                    {
                        "source_id": "chebi",
                        "native_id": "12345",
                        "name": "caffeine",
                        "synonyms": ["caffeine", "ash"],
                        "synonym_types": ["name", "synonym"],
                        "node_type": "compound",
                        "raw_attrs": {"star": 3},
                    },
                ]
            ),
        }
    }
    corrections = Corrections(chebi_lut=ChebiLutCorrections(drop_names=["ash"]))
    create_chemicals_from_chebi(sources, store, lut, corrections, registry)
    assert len(store._entities) == 1
    row = store._entities.iloc[0]
    assert row["entity_type"] == "chemical"
    assert "ash" not in row["synonyms"]
    assert "caffeine" in row["synonyms"]


def test_create_diseases_from_ctd(tmp_path: Path, registry: EntityRegistry) -> None:
    store = _make_store(tmp_path)
    lut = EntityLUT()
    sources = {
        "ctd": {
            "nodes": pd.DataFrame(
                [
                    {
                        "source_id": "ctd",
                        "native_id": "MESH:D001234",
                        "name": "diabetes",
                        "synonyms": ["diabetes", "diabetes mellitus"],
                        "synonym_types": ["label", "synonym"],
                        "node_type": "disease",
                        "raw_attrs": {},
                    },
                ]
            ),
        }
    }
    create_diseases_from_ctd(sources, store, lut, registry)
    assert len(store._entities) == 1
    row = store._entities.iloc[0]
    assert row["entity_type"] == "disease"
    assert row["common_name"] == "diabetes"
