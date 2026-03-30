"""Tests for Pass 2+3 entity resolution (secondary + unlinked)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from src.config.corrections import Corrections
from src.construct.entity_lut import EntityLUT
from src.construct.resolve_secondary import (
    create_unlinked_cdno,
    create_unlinked_fdc_foods,
    create_unlinked_fdc_nutrients,
    link_cdno_to_chebi,
    link_fdc_foods_to_foodon,
    link_fdc_nutrients,
)
from src.stores.entity_store import EntityStore
from src.stores.schema import FILE_ENTITIES, FILE_LUT_CHEMICAL, FILE_LUT_FOOD
from src.utils.json_io import write_json

if TYPE_CHECKING:
    from pathlib import Path


def _make_store_with_entities(tmp_path: Path, entities: list[dict]) -> EntityStore:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir(exist_ok=True)
    write_json(kg_dir / FILE_ENTITIES, entities)
    write_json(kg_dir / FILE_LUT_FOOD, {})
    write_json(kg_dir / FILE_LUT_CHEMICAL, {})
    store = EntityStore(
        path_entities=kg_dir / FILE_ENTITIES,
        path_lut_food=kg_dir / FILE_LUT_FOOD,
        path_lut_chemical=kg_dir / FILE_LUT_CHEMICAL,
    )
    return store


def test_link_cdno_to_chebi(tmp_path: Path) -> None:
    store = _make_store_with_entities(
        tmp_path,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "chemical",
                "common_name": "caffeine",
                "synonyms": ["caffeine"],
                "external_ids": {"chebi": [2345]},
                "scientific_name": "",
                "synonyms_display": {},
            },
        ],
    )
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "cdno": {
            "xrefs": pd.DataFrame(
                [
                    {
                        "source_id": "cdno",
                        "native_id": "CDNO_001",
                        "target_source": "chebi",
                        "target_id": "http://purl.obolibrary.org/obo/CHEBI_2345",
                    },
                ]
            ),
        }
    }
    link_cdno_to_chebi(sources, store)
    ext = store._entities.at["e1", "external_ids"]
    assert "cdno" in ext
    assert "CDNO_001" in ext["cdno"]


def test_link_fdc_foods_to_foodon(tmp_path: Path) -> None:
    store = _make_store_with_entities(
        tmp_path,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "food",
                "common_name": "apple",
                "synonyms": ["apple"],
                "external_ids": {"foodon": ["http://foodon/F1"]},
                "scientific_name": "",
                "synonyms_display": {},
            },
        ],
    )
    linked: set[str] = set()
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "fdc": {
            "xrefs": pd.DataFrame(
                [
                    {
                        "source_id": "fdc",
                        "native_id": "food:100",
                        "target_source": "foodon",
                        "target_id": "http://foodon/F1",
                    },
                ]
            ),
        }
    }
    corrections = Corrections()
    link_fdc_foods_to_foodon(sources, store, corrections, linked)
    ext = store._entities.at["e1", "external_ids"]
    assert "fdc" in ext
    assert 100 in ext["fdc"]
    assert "food:100" in linked


def test_link_fdc_nutrients(tmp_path: Path) -> None:
    store = _make_store_with_entities(
        tmp_path,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "chemical",
                "common_name": "vitamin c",
                "synonyms": ["vitamin c"],
                "external_ids": {"cdno": ["CDNO_X"]},
                "scientific_name": "",
                "synonyms_display": {},
            },
        ],
    )
    linked: set[str] = set()
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "fdc": {
            "nodes": pd.DataFrame(
                [
                    {"native_id": "nutrient:1001", "node_type": "nutrient"},
                ]
            ),
        },
        "cdno": {
            "xrefs": pd.DataFrame(
                [
                    {
                        "source_id": "cdno",
                        "native_id": "CDNO_X",
                        "target_source": "fdc_nutrient",
                        "target_id": "1001",
                    },
                ]
            ),
        },
    }
    link_fdc_nutrients(sources, store, linked)
    ext = store._entities.at["e1", "external_ids"]
    assert "fdc_nutrient" in ext
    assert 1001 in ext["fdc_nutrient"]


def test_create_unlinked_cdno(tmp_path: Path) -> None:
    store = _make_store_with_entities(tmp_path, [])
    lut = EntityLUT()
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "cdno": {
            "nodes": pd.DataFrame(
                [
                    {
                        "source_id": "cdno",
                        "native_id": "CDNO_NEW",
                        "name": "new compound",
                    },
                ]
            ),
        },
    }
    create_unlinked_cdno(sources, store, lut)
    assert len(store._entities) == 1
    assert store._entities.iloc[0]["common_name"] == "new compound"


def test_create_unlinked_fdc_foods(tmp_path: Path) -> None:
    store = _make_store_with_entities(tmp_path, [])
    lut = EntityLUT()
    linked: set[str] = {"food:100"}  # already linked
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "fdc": {
            "nodes": pd.DataFrame(
                [
                    {"native_id": "food:100", "name": "linked", "node_type": "food"},
                    {"native_id": "food:200", "name": "unlinked", "node_type": "food"},
                ]
            ),
        },
    }
    create_unlinked_fdc_foods(sources, store, lut, linked)
    assert len(store._entities) == 1
    assert store._entities.iloc[0]["common_name"] == "unlinked"


def test_create_unlinked_fdc_nutrients(tmp_path: Path) -> None:
    store = _make_store_with_entities(tmp_path, [])
    lut = EntityLUT()
    linked: set[str] = set()
    sources: dict[str, dict[str, pd.DataFrame]] = {
        "fdc": {
            "nodes": pd.DataFrame(
                [
                    {
                        "native_id": "nutrient:999",
                        "name": "mystery",
                        "node_type": "nutrient",
                    },
                ]
            ),
        },
    }
    create_unlinked_fdc_nutrients(sources, store, lut, linked)
    assert len(store._entities) == 1
    assert store._entities.iloc[0]["entity_type"] == "chemical"
