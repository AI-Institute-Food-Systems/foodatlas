"""Tests for DMD entity resolution (Pass 1 / Pass 2 / Pass 3)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.entities.resolve_dmd import (
    create_chemicals_from_dmd,
    create_unlinked_dmd,
    link_dmd,
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


_CHEBI_ENTITY = {
    "foodatlas_id": "e1",
    "entity_type": "chemical",
    "common_name": "caffeine",
    "synonyms": ["caffeine"],
    "external_ids": {"chebi": [123]},
    "scientific_name": "",
}


class TestCreateChemicalsFromDmd:
    """Pass 1: create entities for seeded DMD IDs not yet in the store."""

    def test_creates_at_seeded_id(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        store = _make_store(tmp_path, [])
        registry.register("dmd", "DMD001", "e50")
        lut = EntityLUT()
        create_chemicals_from_dmd(
            _dmd_sources([("DMD001", "newchem")]), store, lut, registry
        )
        assert len(store._entities) == 1
        assert store._entities.index[0] == "e50"

    def test_skips_if_entity_already_in_store(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        store = _make_store(tmp_path, [_CHEBI_ENTITY])
        registry.register("dmd", "DMD001", "e1")
        lut = EntityLUT()
        create_chemicals_from_dmd(
            _dmd_sources([("DMD001", "caffeine")]), store, lut, registry
        )
        # Should not create — entity exists (enrichment is Pass 2).
        assert len(store._entities) == 1

    def test_skips_unregistered(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        create_chemicals_from_dmd(
            _dmd_sources([("DMD999", "new")]), store, lut, registry
        )
        # No registry entry — creation is Pass 3.
        assert len(store._entities) == 0


class TestLinkDmd:
    """Pass 2: enrich existing entities with DMD external IDs."""

    def test_enriches_existing(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [_CHEBI_ENTITY])
        registry.register("dmd", "DMD001", "e1")
        link_dmd(_dmd_sources([("DMD001", "caffeine")]), store, registry)
        ext = store._entities.at["e1", "external_ids"]
        assert "DMD001" in ext["dmd"]

    def test_skips_missing_entity(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        store = _make_store(tmp_path, [])
        registry.register("dmd", "DMD001", "e99")
        link_dmd(_dmd_sources([("DMD001", "caffeine")]), store, registry)
        assert len(store._entities) == 0


class TestCreateUnlinkedDmd:
    """Pass 3: create entities for genuinely new DMD molecules."""

    def test_creates_new(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        create_unlinked_dmd(_dmd_sources([("DMD999", "newchem")]), store, lut, registry)
        assert len(store._entities) == 1
        assert store._entities.iloc[0]["entity_type"] == "chemical"

    def test_skips_registered(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [])
        registry.register("dmd", "DMD001", "e50")
        lut = EntityLUT()
        create_unlinked_dmd(_dmd_sources([("DMD001", "chem")]), store, lut, registry)
        # Has registry entry — handled in Pass 1.
        assert len(store._entities) == 0

    def test_deduplicates(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        sources = _dmd_sources([("DMD001", "c"), ("DMD001", "c")])
        create_unlinked_dmd(sources, store, lut, registry)
        assert len(store._entities) == 1
