"""Tests for DMD primary entity resolution (Pass 1)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.entities.resolve_dmd import create_chemicals_from_dmd
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


def _make_store(tmp_path: Path, entities: list[dict]) -> EntityStore:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir(exist_ok=True)
    df = pd.DataFrame(entities)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(json.dumps)
    df.to_parquet(kg_dir / FILE_ENTITIES, index=False)
    write_json(kg_dir / FILE_LUT_FOOD, {})
    write_json(kg_dir / FILE_LUT_CHEMICAL, {})
    return EntityStore(
        path_entities=kg_dir / FILE_ENTITIES,
        path_lut_food=kg_dir / FILE_LUT_FOOD,
        path_lut_chemical=kg_dir / FILE_LUT_CHEMICAL,
    )


def _dmd_sources(
    names: list[tuple[str, str]],
) -> dict[str, dict[str, pd.DataFrame]]:
    rows = [
        {"native_id": nid, "name": name, "node_type": "molecule", "synonyms": [name]}
        for nid, name in names
    ]
    return {"dmd": {"nodes": pd.DataFrame(rows)}}


def test_enriches_existing_chebi_entity(
    tmp_path: Path, registry: EntityRegistry
) -> None:
    """DMD molecule matching a ChEBI entity enriches it with dmd ext ID."""
    store = _make_store(
        tmp_path,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "chemical",
                "common_name": "caffeine",
                "synonyms": ["caffeine"],
                "external_ids": {"chebi": [123]},
                "scientific_name": "",
            },
        ],
    )
    registry.register("dmd", "DMD001", "e1")
    lut = EntityLUT()
    create_chemicals_from_dmd(
        _dmd_sources([("DMD001", "caffeine")]), store, lut, registry
    )
    assert len(store._entities) == 1
    ext = store._entities.at["e1", "external_ids"]
    assert "DMD001" in ext["dmd"]


def test_reuses_seeded_id(tmp_path: Path, registry: EntityRegistry) -> None:
    """DMD-only molecule with seeded registry entry keeps its old ID."""
    store = _make_store(tmp_path, [])
    registry.register("dmd", "DMD001", "e50")
    lut = EntityLUT()
    create_chemicals_from_dmd(
        _dmd_sources([("DMD001", "newchem")]), store, lut, registry
    )
    assert len(store._entities) == 1
    assert store._entities.index[0] == "e50"


def test_creates_new_entity(tmp_path: Path, registry: EntityRegistry) -> None:
    """DMD molecule with no registry entry gets a fresh ID."""
    store = _make_store(tmp_path, [])
    lut = EntityLUT()
    create_chemicals_from_dmd(
        _dmd_sources([("DMD999", "newchem")]), store, lut, registry
    )
    assert len(store._entities) == 1
    assert store._entities.iloc[0]["entity_type"] == "chemical"
    assert store._entities.iloc[0]["common_name"] == "newchem"


def test_skips_duplicate_native_ids(tmp_path: Path, registry: EntityRegistry) -> None:
    """Duplicate native_ids in source are deduplicated."""
    store = _make_store(tmp_path, [])
    lut = EntityLUT()
    sources = _dmd_sources([("DMD001", "chem"), ("DMD001", "chem")])
    create_chemicals_from_dmd(sources, store, lut, registry)
    assert len(store._entities) == 1


def test_no_dmd_source_is_noop(tmp_path: Path, registry: EntityRegistry) -> None:
    store = _make_store(tmp_path, [])
    lut = EntityLUT()
    create_chemicals_from_dmd({}, store, lut, registry)
    assert len(store._entities) == 0
