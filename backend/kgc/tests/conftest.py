"""Shared test fixtures for KGC tests."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    ENTITY_COLUMNS,
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    TSV_SEP,
)


def _write_lut(path: Path, lut: dict[str, list[str]]) -> None:
    with path.open("w") as f:
        json.dump(lut, f)


@pytest.fixture()
def entities_dir_populated(tmp_path: Path) -> Path:
    """Directory with two entities (apple + vitamin c) and populated LUTs."""
    entities_df = pd.DataFrame(
        [
            {
                "foodatlas_id": "e0",
                "entity_type": "food",
                "common_name": "apple",
                "scientific_name": "malus domestica",
                "synonyms": ["apple", "apples"],
                "external_ids": {"ncbi_taxon_id": 12345},
                "_synonyms_display": ["apple"],
            },
            {
                "foodatlas_id": "e1",
                "entity_type": "chemical",
                "common_name": "vitamin c",
                "scientific_name": "ascorbic acid",
                "synonyms": ["vitamin c", "ascorbic acid"],
                "external_ids": {"pubchem_cid": 54670067},
                "_synonyms_display": ["vitamin c"],
            },
        ]
    )
    entities_df.to_csv(tmp_path / FILE_ENTITIES, sep=TSV_SEP, index=False)

    _write_lut(
        tmp_path / FILE_LUT_FOOD,
        {"apple": ["e0"], "apples": ["e0"]},
    )
    _write_lut(
        tmp_path / FILE_LUT_CHEMICAL,
        {"vitamin c": ["e1"], "ascorbic acid": ["e1"]},
    )
    return tmp_path


@pytest.fixture()
def entities_dir_empty(tmp_path: Path) -> Path:
    """Directory with empty DataFrames using correct schema."""
    pd.DataFrame(columns=ENTITY_COLUMNS).to_csv(
        tmp_path / FILE_ENTITIES, sep=TSV_SEP, index=False
    )
    _write_lut(tmp_path / FILE_LUT_FOOD, {})
    _write_lut(tmp_path / FILE_LUT_CHEMICAL, {})
    return tmp_path


@pytest.fixture()
def entity_store_populated(entities_dir_populated: Path) -> EntityStore:
    return EntityStore(
        path_entities=entities_dir_populated / FILE_ENTITIES,
        path_lut_food=entities_dir_populated / FILE_LUT_FOOD,
        path_lut_chemical=entities_dir_populated / FILE_LUT_CHEMICAL,
    )


@pytest.fixture()
def entity_store_empty(entities_dir_empty: Path) -> EntityStore:
    return EntityStore(
        path_entities=entities_dir_empty / FILE_ENTITIES,
        path_lut_food=entities_dir_empty / FILE_LUT_FOOD,
        path_lut_chemical=entities_dir_empty / FILE_LUT_CHEMICAL,
    )
