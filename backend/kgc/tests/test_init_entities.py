"""Tests for entity initialization from sample ontology data."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from src.initialization.food.init_entities import (
    _remove_plural_display,
    append_foods_from_foodon,
)
from src.models.settings import KGCSettings
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)


def _make_empty_entity_store(tmp_path: Path) -> EntityStore:
    """Create an empty EntityStore with proper files."""
    with (tmp_path / FILE_ENTITIES).open("w") as f:
        json.dump([], f)
    for lut_file in (FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
        (tmp_path / lut_file).write_text("{}")

    return EntityStore(
        path_entities=tmp_path / FILE_ENTITIES,
        path_lut_food=tmp_path / FILE_LUT_FOOD,
        path_lut_chemical=tmp_path / FILE_LUT_CHEMICAL,
    )


@pytest.fixture()
def sample_foodon() -> pd.DataFrame:
    """Minimal FoodOn DataFrame for testing."""
    return pd.DataFrame(
        [
            {
                "is_food": True,
                "parents": [],
                "synonyms": {
                    "label": ["apple"],
                    "label (alternative)": [],
                    "synonym (exact)": [],
                    "synonym": [],
                    "synonym (narrow)": [],
                    "synonym (broad)": [],
                    "taxon": [],
                },
                "derives_from": [],
                "in_taxon": [],
                "derives": [],
                "has_part": [],
            },
            {
                "is_food": True,
                "parents": [],
                "synonyms": {
                    "label": ["banana"],
                    "label (alternative)": ["plantain"],
                    "synonym (exact)": [],
                    "synonym": [],
                    "synonym (narrow)": [],
                    "synonym (broad)": [],
                    "taxon": [],
                },
                "derives_from": [],
                "in_taxon": [],
                "derives": [],
                "has_part": [],
            },
        ],
        index=pd.Index(
            [
                "http://purl.obolibrary.org/obo/FOODON_001",
                "http://purl.obolibrary.org/obo/FOODON_002",
            ],
            name="foodon_id",
        ),
    )


@pytest.fixture()
def sample_lut_food() -> dict[str, str]:
    return {
        "apple": "http://purl.obolibrary.org/obo/FOODON_001",
        "apples": "http://purl.obolibrary.org/obo/FOODON_001",
        "banana": "http://purl.obolibrary.org/obo/FOODON_002",
        "plantain": "http://purl.obolibrary.org/obo/FOODON_002",
        "bananas": "http://purl.obolibrary.org/obo/FOODON_002",
        "plantains": "http://purl.obolibrary.org/obo/FOODON_002",
    }


class TestRemovePluralDisplay:
    def test_single_synonym(self) -> None:
        assert _remove_plural_display(["apple"]) == {"foodon": ["apple"]}

    def test_strips_plural_suffix(self) -> None:
        result = _remove_plural_display(["apple", "apples"])
        assert result == {"foodon": ["apple"]}

    def test_no_plural_found(self) -> None:
        result = _remove_plural_display(["vitamin c", "ascorbic acid"])
        assert result == {"foodon": ["vitamin c", "ascorbic acid"]}


class TestAppendFoodsFromFoodon:
    def test_creates_entities(
        self,
        tmp_path: Path,
        sample_foodon: pd.DataFrame,
        sample_lut_food: dict[str, str],
    ) -> None:
        store = _make_empty_entity_store(tmp_path)
        settings = KGCSettings(kg_dir=str(tmp_path), integration_dir=str(tmp_path))

        with (
            patch(
                "src.initialization.food.init_entities.load_foodon",
                return_value=sample_foodon,
            ),
            patch(
                "src.initialization.food.init_entities.load_lut_food",
                return_value=sample_lut_food,
            ),
        ):
            append_foods_from_foodon(store, settings)

        assert len(store._entities) == 2
        assert store._entities.iloc[0]["entity_type"] == "food"
        assert store._entities.iloc[0]["common_name"] == "apple"

    def test_populates_lut(
        self,
        tmp_path: Path,
        sample_foodon: pd.DataFrame,
        sample_lut_food: dict[str, str],
    ) -> None:
        store = _make_empty_entity_store(tmp_path)
        settings = KGCSettings(kg_dir=str(tmp_path), integration_dir=str(tmp_path))

        with (
            patch(
                "src.initialization.food.init_entities.load_foodon",
                return_value=sample_foodon,
            ),
            patch(
                "src.initialization.food.init_entities.load_lut_food",
                return_value=sample_lut_food,
            ),
        ):
            append_foods_from_foodon(store, settings)

        assert "apple" in store._lut_food
        assert "banana" in store._lut_food
        assert len(store._lut_food["apple"]) == 1

    def test_external_ids_contain_foodon(
        self,
        tmp_path: Path,
        sample_foodon: pd.DataFrame,
        sample_lut_food: dict[str, str],
    ) -> None:
        store = _make_empty_entity_store(tmp_path)
        settings = KGCSettings(kg_dir=str(tmp_path), integration_dir=str(tmp_path))

        with (
            patch(
                "src.initialization.food.init_entities.load_foodon",
                return_value=sample_foodon,
            ),
            patch(
                "src.initialization.food.init_entities.load_lut_food",
                return_value=sample_lut_food,
            ),
        ):
            append_foods_from_foodon(store, settings)

        entity = store._entities.iloc[0]
        assert "foodon" in entity["external_ids"]

    def test_increments_eid(
        self,
        tmp_path: Path,
        sample_foodon: pd.DataFrame,
        sample_lut_food: dict[str, str],
    ) -> None:
        store = _make_empty_entity_store(tmp_path)
        settings = KGCSettings(kg_dir=str(tmp_path), integration_dir=str(tmp_path))

        with (
            patch(
                "src.initialization.food.init_entities.load_foodon",
                return_value=sample_foodon,
            ),
            patch(
                "src.initialization.food.init_entities.load_lut_food",
                return_value=sample_lut_food,
            ),
        ):
            append_foods_from_foodon(store, settings)

        assert store._curr_eid == 3
        assert "e1" in store._entities.index
        assert "e2" in store._entities.index
