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
    xrefs: list[dict] | None = None,
) -> dict[str, dict[str, pd.DataFrame]]:
    rows = [
        {"native_id": nid, "name": name, "node_type": "molecule", "synonyms": [name]}
        for nid, name in names
    ]
    result: dict[str, pd.DataFrame] = {"nodes": pd.DataFrame(rows)}
    if xrefs:
        result["xrefs"] = pd.DataFrame(xrefs)
    else:
        result["xrefs"] = pd.DataFrame(
            columns=["source_id", "native_id", "target_source", "target_id"]
        )
    return {"dmd": result}


_CHEBI_ENTITY = {
    "foodatlas_id": "e1",
    "entity_type": "chemical",
    "common_name": "caffeine",
    "synonyms": ["caffeine"],
    "external_ids": {"chebi": [123]},
    "scientific_name": "",
}

_PUBCHEM_ENTITY = {
    "foodatlas_id": "e2",
    "entity_type": "chemical",
    "common_name": "glucose",
    "synonyms": ["glucose"],
    "external_ids": {"chebi": [456], "pubchem_compound": [5793]},
    "scientific_name": "",
}


class TestCreateChemicalsFromDmd:
    """Pass 1: enrich existing entities with DMD IDs (seeded only)."""

    def test_enriches_existing_entity(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        store = _make_store(tmp_path, [_CHEBI_ENTITY])
        registry.register("dmd", "DMD001", "e1")
        lut = EntityLUT()
        create_chemicals_from_dmd(
            _dmd_sources([("DMD001", "caffeine")]), store, lut, registry
        )
        assert len(store._entities) == 1
        assert "DMD001" in store._entities.at["e1", "external_ids"]["dmd"]

    def test_skips_dmd_only_seed(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        """Seeded DMD molecule with no existing entity → skip to Pass 2/3."""
        store = _make_store(tmp_path, [])
        registry.register("dmd", "DMD001", "e50")
        lut = EntityLUT()
        create_chemicals_from_dmd(
            _dmd_sources([("DMD001", "newchem")]), store, lut, registry
        )
        assert len(store._entities) == 0

    def test_skips_unregistered(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        create_chemicals_from_dmd(
            _dmd_sources([("DMD999", "new")]), store, lut, registry
        )
        assert len(store._entities) == 0


class TestLinkDmd:
    """Pass 2: link DMD molecules to existing entities via xrefs."""

    def test_links_via_chebi(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [_CHEBI_ENTITY])
        registry.register("chebi", "123", "e1")
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD001",
                "target_source": "chebi",
                "target_id": "123",
            }
        ]
        link_dmd(_dmd_sources([("DMD001", "caffeine")], xrefs), store, registry)
        ext = store._entities.at["e1", "external_ids"]
        assert "DMD001" in ext["dmd"]

    def test_links_via_pubchem(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [_PUBCHEM_ENTITY])
        registry.register("chebi", "456", "e2")
        registry.register_alias("pubchem", "5793", "e2")
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD002",
                "target_source": "pubchem_cid",
                "target_id": "5793",
            }
        ]
        link_dmd(_dmd_sources([("DMD002", "glucose")], xrefs), store, registry)
        ext = store._entities.at["e2", "external_ids"]
        assert "DMD002" in ext["dmd"]

    def test_chebi_preferred_over_pubchem(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        """If both ChEBI and PubChem match, ChEBI wins."""
        store = _make_store(tmp_path, [_CHEBI_ENTITY, _PUBCHEM_ENTITY])
        registry.register("chebi", "123", "e1")
        registry.register("chebi", "456", "e2")
        registry.register_alias("pubchem", "5793", "e2")
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD003",
                "target_source": "chebi",
                "target_id": "123",
            },
            {
                "source_id": "dmd",
                "native_id": "DMD003",
                "target_source": "pubchem_cid",
                "target_id": "5793",
            },
        ]
        link_dmd(_dmd_sources([("DMD003", "chem")], xrefs), store, registry)
        # Should link to e1 (ChEBI), not e2 (PubChem).
        assert "DMD003" in store._entities.at["e1", "external_ids"]["dmd"]
        assert "dmd" not in store._entities.at["e2", "external_ids"]

    def test_ambiguous_chebi_links_all(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        """Multiple ChEBI IDs → multiple entities: link to all."""
        entity_a = {
            **_CHEBI_ENTITY,
            "foodatlas_id": "e10",
            "external_ids": {"chebi": [100]},
        }
        entity_b = {
            **_CHEBI_ENTITY,
            "foodatlas_id": "e11",
            "common_name": "caffeine-d",
            "external_ids": {"chebi": [200]},
        }
        store = _make_store(tmp_path, [entity_a, entity_b])
        registry.register("chebi", "100", "e10")
        registry.register("chebi", "200", "e11")
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD004",
                "target_source": "chebi",
                "target_id": "100",
            },
            {
                "source_id": "dmd",
                "native_id": "DMD004",
                "target_source": "chebi",
                "target_id": "200",
            },
        ]
        link_dmd(_dmd_sources([("DMD004", "chem")], xrefs), store, registry)
        assert "DMD004" in store._entities.at["e10", "external_ids"]["dmd"]
        assert "DMD004" in store._entities.at["e11", "external_ids"]["dmd"]

    def test_skips_registered(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [_CHEBI_ENTITY])
        registry.register("chebi", "123", "e1")
        registry.register_alias("dmd", "DMD001", "e1")
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD001",
                "target_source": "chebi",
                "target_id": "123",
            }
        ]
        link_dmd(_dmd_sources([("DMD001", "caffeine")], xrefs), store, registry)
        # Already registered — skipped by Pass 2.
        assert "dmd" not in store._entities.at["e1", "external_ids"]


class TestCreateUnlinkedDmd:
    """Pass 3: create entities for genuinely new DMD molecules."""

    def test_creates_new(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        create_unlinked_dmd(_dmd_sources([("DMD999", "newchem")]), store, lut, registry)
        assert len(store._entities) == 1
        assert store._entities.iloc[0]["entity_type"] == "chemical"

    def test_creates_seeded_dmd_only(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        """Seeded DMD molecule not linked in Pass 2 → created here with seeded ID."""
        store = _make_store(tmp_path, [])
        registry.register("dmd", "DMD001", "e50")
        lut = EntityLUT()
        create_unlinked_dmd(_dmd_sources([("DMD001", "chem")]), store, lut, registry)
        assert len(store._entities) == 1
        assert store._entities.index[0] == "e50"

    def test_deduplicates(self, tmp_path: Path, registry: EntityRegistry) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        sources = _dmd_sources([("DMD001", "c"), ("DMD001", "c")])
        create_unlinked_dmd(sources, store, lut, registry)
        assert len(store._entities) == 1

    def test_disambiguates_duplicate_names(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD001",
                "target_source": "uniprot",
                "target_id": "P02662",
            },
            {
                "source_id": "dmd",
                "native_id": "DMD002",
                "target_source": "uniprot",
                "target_id": "A0A3Q1NG86",
            },
        ]
        sources = _dmd_sources(
            [("DMD001", "Alpha-S1-casein"), ("DMD002", "Alpha-S1-casein")],
            xrefs,
        )
        create_unlinked_dmd(sources, store, lut, registry)
        assert len(store._entities) == 2
        names = sorted(store._entities["common_name"].tolist())
        assert names == [
            "Alpha-S1-casein (UNIPROT:A0A3Q1NG86)",
            "Alpha-S1-casein (UNIPROT:P02662)",
        ]

    def test_disambiguates_against_existing_name(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        existing = {
            **_CHEBI_ENTITY,
            "foodatlas_id": "e100",
            "common_name": "glucose",
            "external_ids": {"chebi": [999]},
        }
        store = _make_store(tmp_path, [existing])
        lut = EntityLUT()
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD005",
                "target_source": "kegg",
                "target_id": "C00031",
            }
        ]
        sources = _dmd_sources([("DMD005", "glucose")], xrefs)
        create_unlinked_dmd(sources, store, lut, registry)
        new_entity = store._entities.loc[store._entities.index != "e100"].iloc[0]
        assert new_entity["common_name"] == "glucose (KEGG:C00031)"

    def test_adds_all_xrefs_to_new_entity(
        self, tmp_path: Path, registry: EntityRegistry
    ) -> None:
        store = _make_store(tmp_path, [])
        lut = EntityLUT()
        xrefs = [
            {
                "source_id": "dmd",
                "native_id": "DMD010",
                "target_source": "uniprot",
                "target_id": "P12345",
            },
            {
                "source_id": "dmd",
                "native_id": "DMD010",
                "target_source": "kegg",
                "target_id": "K00001",
            },
        ]
        sources = _dmd_sources([("DMD010", "someprot")], xrefs)
        create_unlinked_dmd(sources, store, lut, registry)
        ext = store._entities.iloc[0]["external_ids"]
        assert ext["dmd"] == ["DMD010"]
        assert ext["uniprot"] == ["P12345"]
        assert ext["kegg"] == ["K00001"]
