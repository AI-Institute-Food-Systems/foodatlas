"""Tests for the EntityStore class and entity creation helpers."""

from pathlib import Path

import pandas as pd
import pytest
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    ENTITY_COLUMNS,
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)


@pytest.fixture()
def entities(entity_store_populated: EntityStore) -> EntityStore:
    return entity_store_populated


class TestEntityStoreLoad:
    def test_loads_entities(self, entities: EntityStore) -> None:
        assert len(entities._entities) == 2

    def test_curr_eid_increments_past_max(self, entities: EntityStore) -> None:
        assert entities._curr_eid == 2

    def test_lut_food_populated(self, entities: EntityStore) -> None:
        assert "apple" in entities._lut_food
        assert entities._lut_food["apple"] == ["e0"]

    def test_lut_chemical_populated(self, entities: EntityStore) -> None:
        assert "vitamin c" in entities._lut_chemical


class TestEntityLookup:
    def test_get_entity_ids_food(self, entities: EntityStore) -> None:
        assert entities.get_entity_ids("food", "apple") == ["e0"]

    def test_get_entity_ids_missing(self, entities: EntityStore) -> None:
        assert entities.get_entity_ids("food", "banana") == []

    def test_get_entity_ids_chemical(self, entities: EntityStore) -> None:
        assert entities.get_entity_ids("chemical", "vitamin c") == ["e1"]

    def test_get_entity_ids_invalid_type(self, entities: EntityStore) -> None:
        with pytest.raises(ValueError, match="Invalid entity type"):
            entities.get_entity_ids("unknown", "apple")

    def test_get_entity(self, entities: EntityStore) -> None:
        entity = entities.get_entity("e0")
        assert entity["common_name"] == "apple"
        assert entity["entity_type"] == "food"


class TestGetNewNames:
    def test_filters_existing(self, entities: EntityStore) -> None:
        result = entities.get_new_names("food", ["apple", "banana", "cherry"])
        assert result == ["banana", "cherry"]


class TestUpdateSynonyms:
    def test_adds_new_synonym(self, entities: EntityStore) -> None:
        entities.update_entity_synonyms("e0", ["green apple"])
        entity = entities.get_entity("e0")
        assert "green apple" in entity["synonyms"]
        assert "green apple" in entities._lut_food

    def test_skips_existing_synonym(self, entities: EntityStore) -> None:
        entities.update_entity_synonyms("e0", ["apple"])
        entity = entities.get_entity("e0")
        assert entity["synonyms"].count("apple") == 1


class TestUpdateLut:
    def test_adds_to_correct_lut(self, entities: EntityStore) -> None:
        new_df = pd.DataFrame(
            [
                {
                    "entity_type": "food",
                    "common_name": "banana",
                    "synonyms": ["banana", "bananas"],
                    "external_ids": {},
                }
            ],
            index=pd.Index(["e2"], name="foodatlas_id"),
        )
        entities.update_lut(new_df)
        assert "banana" in entities._lut_food
        assert entities._lut_food["banana"] == ["e2"]


class TestSaveReload:
    def test_round_trip(self, entities: EntityStore, tmp_path: Path) -> None:
        out_dir = tmp_path / "output"
        out_dir.mkdir()
        entities.save(out_dir)

        reloaded = EntityStore(
            path_entities=out_dir / FILE_ENTITIES,
            path_lut_food=out_dir / FILE_LUT_FOOD,
            path_lut_chemical=out_dir / FILE_LUT_CHEMICAL,
        )
        assert len(reloaded._entities) == len(entities._entities)
        assert reloaded.get_entity_ids("food", "apple") == ["e0"]
        assert reloaded.get_entity_ids("chemical", "vitamin c") == ["e1"]

    def test_columns_match(self) -> None:
        expected = [
            "foodatlas_id",
            "entity_type",
            "common_name",
            "scientific_name",
            "synonyms",
            "external_ids",
        ]
        assert expected == ENTITY_COLUMNS
