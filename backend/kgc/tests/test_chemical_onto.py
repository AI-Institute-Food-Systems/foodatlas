"""Tests for chemical ontology creation."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
from src.integration.ontologies.chemical import (
    _build_chebi_to_fa_map,
    create_chemical_ontology,
)
from src.models.settings import KGCSettings
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_CHEMICAL_ONTOLOGY,
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)


def _ent(fid: str, etype: str, name: str, syns: list[str], ext: dict) -> dict:
    return {
        "foodatlas_id": fid,
        "entity_type": etype,
        "common_name": name,
        "scientific_name": None,
        "synonyms": syns,
        "external_ids": ext,
    }


def _chem(fid: str, name: str, ext: dict[str, Any]) -> dict:
    return _ent(fid, "chemical", name, [name], ext)


def _wj(path: Path, data: object) -> None:
    with path.open("w") as f:
        json.dump(data, f, ensure_ascii=False)


def _store(tp: Path, ents: list[dict], lut_c: dict | None = None) -> EntityStore:
    _wj(tp / FILE_ENTITIES, ents)
    _wj(tp / FILE_LUT_FOOD, {})
    _wj(tp / FILE_LUT_CHEMICAL, lut_c or {})
    return EntityStore(
        path_entities=tp / FILE_ENTITIES,
        path_lut_food=tp / FILE_LUT_FOOD,
        path_lut_chemical=tp / FILE_LUT_CHEMICAL,
    )


def _cfg(tp: Path) -> KGCSettings:
    return KGCSettings(kg_dir=str(tp), data_dir=str(tp), integration_dir=str(tp))


class TestBuildChebiToFaMap:
    def test_maps_chebi_to_fa(self, tmp_path: Path) -> None:
        ents = [
            _chem("e1", "water", {"chebi": [15377]}),
            _ent("e2", "food", "apple", ["apple"], {}),
        ]
        assert _build_chebi_to_fa_map(_store(tmp_path, ents)) == {15377: "e1"}


class TestCreateChemicalOntology:
    def test_creates_is_a_triplets(self, tmp_path: Path) -> None:
        ents = [
            _chem("e1", "water", {"chebi": [100]}),
            _chem("e2", "liquid", {"chebi": [200]}),
        ]
        store = _store(tmp_path, ents)
        rels = pd.DataFrame(
            {
                "TYPE": ["is_a", "has_part", "is_a"],
                "INIT_ID": [100, 100, 999],
                "FINAL_ID": [200, 200, 200],
            }
        )
        with patch("pandas.read_csv", return_value=rels):
            r = create_chemical_ontology(store, _cfg(tmp_path))
        assert len(r) == 1
        assert r.iloc[0]["head_id"] == "e2" and r.iloc[0]["tail_id"] == "e1"
        assert r.iloc[0]["foodatlas_id"] == "co1"
        with (tmp_path / FILE_CHEMICAL_ONTOLOGY).open() as f:
            assert len(json.load(f)) == 1

    def test_skips_unknown_chebi_ids(self, tmp_path: Path) -> None:
        rels = pd.DataFrame({"TYPE": ["is_a"], "INIT_ID": [1], "FINAL_ID": [2]})
        with patch("pandas.read_csv", return_value=rels):
            assert (
                len(create_chemical_ontology(_store(tmp_path, []), _cfg(tmp_path))) == 0
            )
