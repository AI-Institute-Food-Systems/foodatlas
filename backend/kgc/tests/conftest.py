"""Shared test fixtures for KGC tests."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.constructor.knowledge_graph import KnowledgeGraph
from src.models.settings import KGCSettings
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    ENTITY_COLUMNS,
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_METADATA_CONTAINS,
    FILE_TRIPLETS,
    TSV_SEP,
)


def _write_lut(path: Path, lut: dict[str, list[str]]) -> None:
    with path.open("w") as f:
        json.dump(lut, f)


def _make_kg_dir(tmp_path: Path) -> Path:
    """Create a minimal KG directory with fixture TSVs and LUTs."""
    entities = pd.DataFrame(
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
    entities.to_csv(tmp_path / FILE_ENTITIES, sep=TSV_SEP, index=False)

    triplets = pd.DataFrame(
        [
            {
                "foodatlas_id": "t0",
                "head_id": "e0",
                "relationship_id": "r1",
                "tail_id": "e1",
                "metadata_ids": ["mc0"],
            },
        ]
    )
    triplets.to_csv(tmp_path / FILE_TRIPLETS, sep=TSV_SEP, index=False)

    metadata = pd.DataFrame(
        [
            {
                "foodatlas_id": "mc0",
                "conc_value": 1.5,
                "conc_unit": "mg/g",
                "food_part": "peel",
                "food_processing": "raw",
                "source": "fdc",
                "reference": ["ref1"],
                "entity_linking_method": "exact",
                "quality_score": 0.95,
                "_food_name": "apple",
                "_chemical_name": "vitamin c",
                "_conc": "1.5 mg/g",
                "_food_part": "peel",
            },
        ]
    )
    metadata.to_csv(tmp_path / FILE_METADATA_CONTAINS, sep=TSV_SEP, index=False)

    _write_lut(
        tmp_path / FILE_LUT_FOOD,
        {"apple": ["e0"], "apples": ["e0"]},
    )
    _write_lut(
        tmp_path / FILE_LUT_CHEMICAL,
        {"vitamin c": ["e1"], "ascorbic acid": ["e1"]},
    )
    return tmp_path


# ── Entity fixtures ──────────────────────────────────────────────────


@pytest.fixture()
def entities_dir_populated(tmp_path: Path) -> Path:
    """Directory with two entities (apple + vitamin c) and populated LUTs."""
    return _make_kg_dir(tmp_path)


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


# ── KnowledgeGraph fixtures ─────────────────────────────────────────


@pytest.fixture()
def kg_dir(tmp_path: Path) -> Path:
    return _make_kg_dir(tmp_path)


@pytest.fixture()
def kg(kg_dir: Path) -> KnowledgeGraph:
    settings = KGCSettings(kg_dir=str(kg_dir), cache_dir=str(kg_dir / "_cache"))
    return KnowledgeGraph(settings)
