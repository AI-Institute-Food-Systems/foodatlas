"""Tests for synonym disambiguation logic."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.constructor.disambiguation import disambiguate_synonyms
from src.stores.entity_store import EntityStore
from src.stores.schema import TSV_SEP


def _write_lut(path: Path, data: dict) -> None:
    with path.open("w") as f:
        json.dump(data, f)


def _make_entity_store(
    tmp_path: Path,
    entities_data: list[dict],
    lut_food: dict[str, list[str]],
    lut_chemical: dict[str, list[str]],
) -> EntityStore:
    df = pd.DataFrame(entities_data)
    df.to_csv(tmp_path / "entities.tsv", sep=TSV_SEP, index=False)
    _write_lut(tmp_path / "lookup_table_food.json", lut_food)
    _write_lut(tmp_path / "lookup_table_chemical.json", lut_chemical)
    return EntityStore(
        path_entities=tmp_path / "entities.tsv",
        path_lut_food=tmp_path / "lookup_table_food.json",
        path_lut_chemical=tmp_path / "lookup_table_chemical.json",
    )


class TestNoAmbiguity:
    def test_no_changes_when_unique(self, tmp_path: Path) -> None:
        store = _make_entity_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e0",
                    "entity_type": "food",
                    "common_name": "apple",
                    "scientific_name": "",
                    "synonyms": ["apple"],
                    "external_ids": {},
                    "_synonyms_display": ["apple"],
                },
            ],
            {"apple": ["e0"]},
            {},
        )
        disambiguate_synonyms(store)
        assert len(store._entities) == 1


class TestPlaceholderCreation:
    @pytest.fixture()
    def ambiguous_store(self, tmp_path: Path) -> EntityStore:
        return _make_entity_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e0",
                    "entity_type": "food",
                    "common_name": "orange",
                    "scientific_name": "",
                    "synonyms": ["orange", "navel orange"],
                    "external_ids": {},
                    "_synonyms_display": ["orange"],
                },
                {
                    "foodatlas_id": "e1",
                    "entity_type": "food",
                    "common_name": "orange",
                    "scientific_name": "",
                    "synonyms": ["orange", "blood orange"],
                    "external_ids": {},
                    "_synonyms_display": ["orange"],
                },
            ],
            {
                "orange": ["e0", "e1"],
                "navel orange": ["e0"],
                "blood orange": ["e1"],
            },
            {},
        )

    def test_creates_placeholder(self, ambiguous_store: EntityStore) -> None:
        disambiguate_synonyms(ambiguous_store)
        assert len(ambiguous_store._entities) == 3

    def test_placeholder_has_correct_type(self, ambiguous_store: EntityStore) -> None:
        disambiguate_synonyms(ambiguous_store)
        placeholder = ambiguous_store._entities.loc["e2"]
        assert placeholder["entity_type"] == "food"
        assert placeholder["common_name"] == "orange"

    def test_placeholder_points_to_originals(
        self, ambiguous_store: EntityStore
    ) -> None:
        disambiguate_synonyms(ambiguous_store)
        placeholder = ambiguous_store._entities.loc["e2"]
        assert "e0" in placeholder["external_ids"]["_placeholder_to"]
        assert "e1" in placeholder["external_ids"]["_placeholder_to"]

    def test_originals_have_synonym_removed(self, ambiguous_store: EntityStore) -> None:
        disambiguate_synonyms(ambiguous_store)
        e0 = ambiguous_store._entities.loc["e0"]
        e1 = ambiguous_store._entities.loc["e1"]
        assert "orange" not in e0["synonyms"]
        assert "orange" not in e1["synonyms"]

    def test_lut_points_to_placeholder(self, ambiguous_store: EntityStore) -> None:
        disambiguate_synonyms(ambiguous_store)
        assert ambiguous_store._lut_food["orange"] == ["e2"]

    def test_originals_have_back_pointers(self, ambiguous_store: EntityStore) -> None:
        disambiguate_synonyms(ambiguous_store)
        e0 = ambiguous_store._entities.loc["e0"]
        assert "e2" in e0["external_ids"]["_placeholder_from"]

    def test_common_name_updated_after_removal(
        self, ambiguous_store: EntityStore
    ) -> None:
        disambiguate_synonyms(ambiguous_store)
        e0 = ambiguous_store._entities.loc["e0"]
        assert e0["common_name"] in e0["synonyms"]


class TestExistingPlaceholder:
    def test_extends_existing_placeholder(self, tmp_path: Path) -> None:
        store = _make_entity_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e0",
                    "entity_type": "food",
                    "common_name": "citrus",
                    "scientific_name": "",
                    "synonyms": ["citrus"],
                    "external_ids": {
                        "_placeholder_to": ["e1", "e2"],
                    },
                    "_synonyms_display": ["citrus"],
                },
                {
                    "foodatlas_id": "e1",
                    "entity_type": "food",
                    "common_name": "lemon",
                    "scientific_name": "",
                    "synonyms": ["lemon"],
                    "external_ids": {},
                    "_synonyms_display": ["lemon"],
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "food",
                    "common_name": "lime",
                    "scientific_name": "",
                    "synonyms": ["lime"],
                    "external_ids": {},
                    "_synonyms_display": ["lime"],
                },
                {
                    "foodatlas_id": "e3",
                    "entity_type": "food",
                    "common_name": "citrus",
                    "scientific_name": "",
                    "synonyms": ["citrus", "grapefruit"],
                    "external_ids": {},
                    "_synonyms_display": ["citrus"],
                },
            ],
            {
                "citrus": ["e0", "e3"],
                "lemon": ["e1"],
                "lime": ["e2"],
                "grapefruit": ["e3"],
            },
            {},
        )
        disambiguate_synonyms(store)
        placeholder = store._entities.loc["e0"]
        assert "e3" in placeholder["external_ids"]["_placeholder_to"]
        assert store._lut_food["citrus"] == ["e0"]
