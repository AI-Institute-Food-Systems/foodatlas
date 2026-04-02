"""Tests for PubChem/MeSH cross-reference linking."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.entities.link_xrefs import link_mesh_to_chebi, link_pubchem_to_chebi
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


def test_link_pubchem_to_chebi(tmp_path: Path, registry: EntityRegistry) -> None:
    store = _make_store(
        tmp_path,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "chemical",
                "common_name": "aspirin",
                "synonyms": ["aspirin"],
                "external_ids": {"chebi": [15365]},
                "scientific_name": "",
            },
        ],
    )
    sources = {
        "pubchem": {
            "xrefs": pd.DataFrame(
                [
                    {
                        "source_id": "pubchem",
                        "native_id": "2244",
                        "target_source": "chebi",
                        "target_id": "CHEBI:15365",
                    },
                ]
            ),
        },
    }
    link_pubchem_to_chebi(sources, store, registry, merges={})
    ext = store._entities.at["e1", "external_ids"]
    assert 2244 in ext["pubchem_compound"]


def test_link_pubchem_no_pubchem_source(
    tmp_path: Path, registry: EntityRegistry
) -> None:
    store = _make_store(tmp_path, [])
    link_pubchem_to_chebi({}, store, registry, merges={})


def test_link_mesh_to_chebi(tmp_path: Path, registry: EntityRegistry) -> None:
    store = _make_store(
        tmp_path,
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "chemical",
                "common_name": "aspirin",
                "synonyms": ["aspirin"],
                "external_ids": {"chebi": [15365], "pubchem_compound": [2244]},
                "scientific_name": "",
            },
        ],
    )
    sources = {
        "pubchem": {
            "xrefs": pd.DataFrame(
                [
                    {
                        "source_id": "pubchem",
                        "native_id": "2244",
                        "target_source": "mesh_term",
                        "target_id": "Aspirin",
                    },
                ]
            ),
        },
        "mesh": {
            "nodes": pd.DataFrame(
                [
                    {
                        "source_id": "mesh",
                        "native_id": "D001241",
                        "name": "Aspirin",
                        "synonyms": [],
                        "synonym_types": [],
                        "node_type": "descriptor",
                        "raw_attrs": {},
                    },
                ]
            ),
        },
    }
    link_mesh_to_chebi(sources, store, registry, merges={})
    ext = store._entities.at["e1", "external_ids"]
    assert "D001241" in ext["mesh"]


def test_link_mesh_no_mesh_source(tmp_path: Path, registry: EntityRegistry) -> None:
    store = _make_store(tmp_path, [])
    link_mesh_to_chebi(
        {"pubchem": {"xrefs": pd.DataFrame()}}, store, registry, merges={}
    )
