"""Tests for EntityResolver (three-pass orchestrator)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from src.config.corrections import Corrections
from src.construct.entity_resolver import EntityResolver
from src.stores.schema import FILE_ENTITIES, FILE_LUT_CHEMICAL, FILE_LUT_FOOD
from src.utils.json_io import write_json

if TYPE_CHECKING:
    from pathlib import Path


def _setup_kg_dir(tmp_path: Path) -> Path:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir()
    write_json(kg_dir / FILE_ENTITIES, [])
    write_json(kg_dir / FILE_LUT_FOOD, {})
    write_json(kg_dir / FILE_LUT_CHEMICAL, {})
    return kg_dir


def _make_foodon_source() -> dict[str, pd.DataFrame]:
    return {
        "nodes": pd.DataFrame(
            [
                {
                    "source_id": "foodon",
                    "native_id": "http://foodon/FOOD_001",
                    "name": "apple",
                    "synonyms": ["apple"],
                    "synonym_types": ["label"],
                    "node_type": "class",
                    "raw_attrs": {},
                    "is_food": True,
                },
            ]
        ),
        "edges": pd.DataFrame(
            columns=[
                "source_id",
                "head_native_id",
                "tail_native_id",
                "edge_type",
                "raw_attrs",
            ]
        ),
    }


def _make_chebi_source() -> dict[str, pd.DataFrame]:
    return {
        "nodes": pd.DataFrame(
            [
                {
                    "source_id": "chebi",
                    "native_id": "12345",
                    "name": "caffeine",
                    "synonyms": ["caffeine", "1,3,7-trimethylxanthine"],
                    "synonym_types": ["name", "iupac"],
                    "node_type": "compound",
                    "raw_attrs": {"star": 3},
                },
            ]
        ),
        "edges": pd.DataFrame(
            columns=[
                "source_id",
                "head_native_id",
                "tail_native_id",
                "edge_type",
                "raw_attrs",
            ]
        ),
    }


def _make_ctd_source() -> dict[str, pd.DataFrame]:
    return {
        "nodes": pd.DataFrame(
            [
                {
                    "source_id": "ctd",
                    "native_id": "MESH:D001234",
                    "name": "diabetes",
                    "synonyms": ["diabetes"],
                    "synonym_types": ["label"],
                    "node_type": "disease",
                    "raw_attrs": {},
                },
            ]
        ),
        "edges": pd.DataFrame(
            columns=[
                "source_id",
                "head_native_id",
                "tail_native_id",
                "edge_type",
                "raw_attrs",
            ]
        ),
    }


def test_resolver_creates_entities(tmp_path: Path) -> None:
    kg_dir = _setup_kg_dir(tmp_path)
    corrections = Corrections()
    resolver = EntityResolver(kg_dir, corrections)

    sources = {
        "foodon": _make_foodon_source(),
        "chebi": _make_chebi_source(),
        "ctd": _make_ctd_source(),
    }
    store = resolver.resolve(sources)

    assert len(store._entities) == 3
    types = set(store._entities["entity_type"])
    assert types == {"food", "chemical", "disease"}


def test_resolver_builds_lut(tmp_path: Path) -> None:
    kg_dir = _setup_kg_dir(tmp_path)
    resolver = EntityResolver(kg_dir, Corrections())
    sources = {"foodon": _make_foodon_source()}
    resolver.resolve(sources)

    assert resolver.lut.lookup("food", "apple")
    assert "apple" in resolver.entity_store._lut_food


def test_resolver_pass3_unlinked_fdc(tmp_path: Path) -> None:
    kg_dir = _setup_kg_dir(tmp_path)
    resolver = EntityResolver(kg_dir, Corrections())
    sources = {
        "fdc": {
            "nodes": pd.DataFrame(
                [
                    {
                        "source_id": "fdc",
                        "native_id": "food:999",
                        "name": "mystery food",
                        "synonyms": ["mystery food"],
                        "synonym_types": ["label"],
                        "node_type": "food",
                        "raw_attrs": {},
                    },
                ]
            ),
            "xrefs": pd.DataFrame(
                columns=["source_id", "native_id", "target_source", "target_id"]
            ),
        },
    }
    store = resolver.resolve(sources)
    assert len(store._entities) == 1
    assert store._entities.iloc[0]["entity_type"] == "food"


def test_resolver_empty_sources(tmp_path: Path) -> None:
    kg_dir = _setup_kg_dir(tmp_path)
    resolver = EntityResolver(kg_dir, Corrections())
    store = resolver.resolve({})
    assert len(store._entities) == 0
