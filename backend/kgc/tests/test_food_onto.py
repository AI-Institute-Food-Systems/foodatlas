"""Tests for food ontology creation."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from src.integration.triplets.food_food.foodon import (
    _build_foodon_to_fa_map,
    _traverse_hierarchy,
    create_food_ontology,
)
from src.models.settings import KGCSettings
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_FOOD_ONTOLOGY,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)

FA = "http://purl.obolibrary.org/obo/FOODON_001"
FB = "http://purl.obolibrary.org/obo/FOODON_002"


def _wj(path: Path, data: object) -> None:
    with path.open("w") as f:
        json.dump(data, f)


def _ent(eid: str, name: str, ext: dict, etype: str = "food") -> dict:
    return {
        "foodatlas_id": eid,
        "entity_type": etype,
        "common_name": name,
        "scientific_name": None,
        "synonyms": [name],
        "external_ids": ext,
        "_synonyms_display": {},
    }


def _store(tmp: Path, ents: list | None = None) -> EntityStore:
    _wj(tmp / FILE_ENTITIES, ents or [])
    _wj(tmp / FILE_LUT_FOOD, {})
    _wj(tmp / FILE_LUT_CHEMICAL, {})
    return EntityStore(
        path_entities=tmp / FILE_ENTITIES,
        path_lut_food=tmp / FILE_LUT_FOOD,
        path_lut_chemical=tmp / FILE_LUT_CHEMICAL,
    )


def _settings(tmp: Path) -> KGCSettings:
    return KGCSettings(
        kg_dir=str(tmp),
        data_dir=str(tmp),
        pipeline={"stages": {"data_cleaning": {"output_dir": str(tmp)}}},
    )


def _syns(label: list[str] | None = None) -> dict:
    base: dict[str, list[str]] = {
        k: []
        for k in [
            "label",
            "label (alternative)",
            "synonym (exact)",
            "synonym",
            "synonym (narrow)",
            "synonym (broad)",
        ]
    }
    if label:
        base["label"] = label
    return base


def _fon(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).set_index("foodon_id")


def _fon_row(
    fid: str,
    label: str,
    *,
    food: bool = True,
    parents: list | None = None,
) -> dict:
    return {
        "foodon_id": fid,
        "is_food": food,
        "is_organism": not food,
        "parents": parents or [],
        "synonyms": _syns([label]),
        "derives": [],
        "has_part": [],
        "derives_from": [],
        "in_taxon": [],
    }


@pytest.fixture()
def foodon2() -> pd.DataFrame:
    return _fon([_fon_row(FA, "apple", parents=[FB]), _fon_row(FB, "banana")])


class TestBuildFoodonToFaMap:
    def test_maps(self, tmp_path: Path) -> None:
        store = _store(tmp_path, [_ent("e1", "apple", {"foodon": [FA]})])
        assert _build_foodon_to_fa_map(store) == {FA: "e1"}

    def test_skips_non_foodon(self, tmp_path: Path) -> None:
        store = _store(tmp_path, [_ent("e1", "vc", {"pubchem": [1]}, "chemical")])
        assert _build_foodon_to_fa_map(store) == {}


class TestTraverseHierarchy:
    def test_collects_is_a(self, foodon2: pd.DataFrame) -> None:
        rows = _traverse_hierarchy(foodon2, {FA: "e1", FB: "e2"})
        assert len(rows) == 1
        assert rows[0] == {
            "foodatlas_id": None,
            "head_id": "e1",
            "relationship_id": "r2",
            "tail_id": "e2",
            "source": "foodon",
        }

    def test_no_parents(self) -> None:
        foodon = _fon([_fon_row(FA, "apple")])
        assert _traverse_hierarchy(foodon, {FA: "e1"}) == []


class TestCreateFoodOntology:
    def test_end_to_end(self, tmp_path: Path, foodon2: pd.DataFrame) -> None:
        ents = [
            _ent("e1", "apple", {"foodon": [FA]}),
            _ent("e2", "banana", {"foodon": [FB]}),
        ]
        store = _store(tmp_path, ents)
        with patch(
            "src.integration.triplets.food_food.foodon.load_foodon",
            return_value=foodon2,
        ):
            result = create_food_ontology(store, _settings(tmp_path))
        assert len(result) == 1
        assert (tmp_path / FILE_FOOD_ONTOLOGY).exists()
        with (tmp_path / FILE_FOOD_ONTOLOGY).open() as f:
            saved = json.load(f)
        assert saved[0]["head_id"] == "e1" and saved[0]["tail_id"] == "e2"
