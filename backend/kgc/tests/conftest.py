"""Shared test fixtures for KGC tests."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.models.settings import KGCSettings
from src.pipeline.knowledge_graph import KnowledgeGraph
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_ATTESTATIONS,
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_TRIPLETS,
)


def _write_entities_parquet(path: Path, entities: list[dict]) -> None:
    """Write entity records as parquet with JSON-serialized complex columns."""
    df = pd.DataFrame(entities)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(json.dumps)
    df.to_parquet(path, index=False)


def _write_lut(path: Path, lut: dict[str, list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(lut, f)


def _write_json(path: Path, data: object) -> None:
    with path.open("w") as f:
        json.dump(data, f, ensure_ascii=False)


def _make_kg_dir(tmp_path: Path) -> Path:
    """Create a minimal KG directory with fixture files."""
    entities = [
        {
            "foodatlas_id": "e0",
            "entity_type": "food",
            "common_name": "apple",
            "scientific_name": "malus domestica",
            "synonyms": ["apple", "apples"],
            "external_ids": {"ncbi_taxon_id": 12345},
        },
        {
            "foodatlas_id": "e1",
            "entity_type": "chemical",
            "common_name": "vitamin c",
            "scientific_name": "ascorbic acid",
            "synonyms": ["vitamin c", "ascorbic acid"],
            "external_ids": {"pubchem_cid": 54670067},
        },
    ]
    _write_entities_parquet(tmp_path / FILE_ENTITIES, entities)

    triplets = [
        {
            "head_id": "e0",
            "relationship_id": "r1",
            "tail_id": "e1",
            "source": "fdc",
            "attestation_ids": json.dumps(["at_test0"]),
        },
    ]
    pd.DataFrame(triplets).to_parquet(tmp_path / FILE_TRIPLETS, index=False)

    evidence = [
        {
            "evidence_id": "ev_test0",
            "source_type": "fdc",
            "reference": '{"url": "https://fdc.nal.usda.gov/test"}',
        },
    ]
    pd.DataFrame(evidence).to_parquet(tmp_path / FILE_EVIDENCE, index=False)

    attestations = [
        {
            "attestation_id": "at_test0",
            "evidence_id": "ev_test0",
            "source": "fdc",
            "head_name_raw": "apple",
            "tail_name_raw": "vitamin c",
            "conc_value": 1.5,
            "conc_unit": "mg/g",
            "food_part": "peel",
            "food_processing": "raw",
            "filter_score": 0.95,
            "validated": False,
            "validated_correct": True,
        },
    ]
    pd.DataFrame(attestations).to_parquet(tmp_path / FILE_ATTESTATIONS, index=False)

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
    """Directory with empty parquet + JSON LUT files."""
    pd.DataFrame().to_parquet(tmp_path / FILE_ENTITIES)
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
