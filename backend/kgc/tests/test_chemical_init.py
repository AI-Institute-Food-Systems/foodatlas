"""Tests for chemical initialization: loaders, init_entities, init_onto."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
from src.initialization.chemical.init_entities import (
    _add_to_lut,
    append_entities_from_cdno,
    append_entities_from_chebi,
    append_entities_from_fdc,
)
from src.initialization.chemical.init_onto import (
    _build_chebi_to_fa_map,
    create_chemical_ontology,
)
from src.initialization.chemical.loaders import (
    load_cdno,
    load_fdc_nutrient,
    load_mapper_chebi_id_to_names,
    load_mapper_chebi_id_to_pubchem_cid,
    load_mapper_name_to_chebi_id,
    load_mapper_name_to_mesh_id,
    load_mapper_pubchem_cid_to_mesh_id,
    load_mesh,
)
from src.models.settings import KGCSettings
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_CHEMICAL_ONTOLOGY,
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)

_IE = "src.initialization.chemical.init_entities"


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


# ── loaders ──────────────────────────────────────────────────────────


class TestLoaders:
    def test_name_to_chebi_filters_ash(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            {"NAME": ["water", "ash", "ethanol"], "CHEBI_ID": [[1], [2], [3]]}
        )
        with patch("pandas.read_parquet", return_value=df):
            r = load_mapper_name_to_chebi_id(_cfg(tmp_path))
        assert list(r["NAME"]) == ["water", "ethanol"]

    def test_chebi_id_to_names_inverts(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"NAME": ["alpha", "beta"], "CHEBI_ID": [[10], [10]]})
        with patch("pandas.read_parquet", return_value=df):
            r = load_mapper_chebi_id_to_names(_cfg(tmp_path))
        assert r["CHEBI_ID"].iloc[0] == 10
        assert sorted(r["NAME"].iloc[0]) == ["alpha", "beta"]

    def test_cdno_parses_columns(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            {
                "index": ["http://x/CDNO_0001"],
                "chebi_id": ["http://purl.obolibrary.org/obo/CHEBI_42"],
                "fdc_nutrient_ids": [[100]],
                "label": ["concentration of glucose in material entity"],
            }
        )
        with patch("pandas.read_parquet", return_value=df):
            r = load_cdno(_cfg(tmp_path))
        assert r["cdno_id"].iloc[0] == "CDNO_0001"
        assert r["chebi_id"].iloc[0] == 42
        assert r["label"].iloc[0] == "glucose"

    def test_cdno_nitrogen_override(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            {
                "index": ["http://x/CDNO_0002"],
                "chebi_id": [None],
                "fdc_nutrient_ids": [[300]],
                "label": ["concentration of nitrogen atom in material entity"],
            }
        )
        with patch("pandas.read_parquet", return_value=df):
            assert load_cdno(_cfg(tmp_path))["chebi_id"].iloc[0] == 29351

    def test_fdc_nutrient_filters_and_renames(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            {
                "id": [1001, 2048, 1008, 1062, 2047],
                "name": ["Iron", "X", "Y", "Z", "Energy (Atwater)"],
            }
        )
        with patch("pandas.read_csv", return_value=df):
            r = load_fdc_nutrient(_cfg(tmp_path))
        assert 2048 not in r.index and 1008 not in r.index
        assert r.loc[2047, "name"] == "energy"
        assert r.loc[1001, "name"] == "iron"

    def test_name_to_mesh_id(self, tmp_path: Path) -> None:
        d = pd.DataFrame({"name": ["aspirin"], "id": ["D001"]})
        s = pd.DataFrame({"name": ["ibuprofen"], "id": ["C002"]})
        with patch("pandas.read_parquet", side_effect=[d, s]):
            r = load_mapper_name_to_mesh_id(_cfg(tmp_path))
        assert r["aspirin"] == "D001" and r["ibuprofen"] == "C002"

    def test_mesh_lowercases_deduplicates(self, tmp_path: Path) -> None:
        d = pd.DataFrame({"id": ["D001"], "synonyms": [["Aspirin", "aspirin", "ASA"]]})
        s = pd.DataFrame({"id": ["C002"], "synonyms": [["Ibu"]]})
        with patch("pandas.read_parquet", side_effect=[d, s]):
            assert load_mesh(_cfg(tmp_path))["D001"] == ["aspirin", "asa"]

    def test_pubchem_cid_to_mesh_id(self, tmp_path: Path) -> None:
        d = pd.DataFrame({"name": ["aspirin"], "id": ["D001"]})
        s = pd.DataFrame({"name": [], "id": []})
        csv = pd.DataFrame(
            {"cid": [123], "mesh_term": ["aspirin"], "mesh_term_alt": [""]}
        )
        with (
            patch("pandas.read_parquet", side_effect=[d, s]),
            patch("pandas.read_csv", return_value=csv),
        ):
            assert load_mapper_pubchem_cid_to_mesh_id(_cfg(tmp_path)).loc[123] == "D001"

    def test_chebi_id_to_pubchem_cid(self, tmp_path: Path) -> None:
        df = pd.DataFrame({"registry_id": ["CHEBI:15377"], "cid": [962]})
        with patch("pandas.read_parquet", return_value=df):
            assert load_mapper_chebi_id_to_pubchem_cid(_cfg(tmp_path)).loc[15377] == 962


# ── init_entities ────────────────────────────────────────────────────


class TestAddToLut:
    def test_adds_synonyms(self) -> None:
        lut: dict[str, list[str]] = {}
        _add_to_lut(
            pd.Series({"entity_type": "chemical", "synonyms": ["water"]}, name="e0"),
            lut,
        )
        assert lut == {"water": ["e0"]}

    def test_skips_non_chemical(self) -> None:
        lut: dict[str, list[str]] = {}
        _add_to_lut(
            pd.Series({"entity_type": "food", "synonyms": ["apple"]}, name="e0"), lut
        )
        assert lut == {}

    def test_raises_on_duplicate(self) -> None:
        with pytest.raises(ValueError, match="Duplicate synonym"):
            _add_to_lut(
                pd.Series(
                    {"entity_type": "chemical", "synonyms": ["water"]}, name="e1"
                ),
                {"water": ["e0"]},
            )


class TestAppendFromChebi:
    def test_creates_entities_and_placeholders(self, tmp_path: Path) -> None:
        store = _store(tmp_path, [])
        c2n = pd.DataFrame(
            {"CHEBI_ID": [10, 20], "NAME": [["water", "ambig"], ["ethanol", "ambig"]]}
        )
        n2c = pd.DataFrame(
            {"NAME": ["water", "ethanol", "ambig"], "CHEBI_ID": [[10], [20], [10, 20]]}
        )
        with (
            patch(f"{_IE}.load_mapper_chebi_id_to_names", return_value=c2n),
            patch(f"{_IE}.load_mapper_name_to_chebi_id", return_value=n2c),
        ):
            append_entities_from_chebi(store, _cfg(tmp_path))
        assert len(store._entities) == 3
        phs = store._entities[
            store._entities["external_ids"].apply(lambda x: "_placeholder_to" in x)
        ]
        assert len(phs) == 1


class TestAppendFromCdno:
    def test_links_existing_and_creates_new(self, tmp_path: Path) -> None:
        store = _store(
            tmp_path, [_chem("e1", "water", {"chebi": [10]})], {"water": ["e1"]}
        )
        cdno = pd.DataFrame(
            {
                "cdno_id": ["CDNO_01", "CDNO_02"],
                "label": ["water", "fiber"],
                "chebi_id": pd.array([10, pd.NA], dtype="Int64"),
                "fdc_nutrient_ids": [[100], [200]],
            }
        )
        with patch(f"{_IE}.load_cdno", return_value=cdno):
            append_entities_from_cdno(store, _cfg(tmp_path))
        assert store._entities.at["e1", "external_ids"]["cdno"] == ["CDNO_01"]
        assert len(store._entities) == 2
        assert store._entities.loc["e2"]["common_name"] == "fiber"

    def test_new_cdno_with_chebi_id(self, tmp_path: Path) -> None:
        store = _store(tmp_path, [])
        cdno = pd.DataFrame(
            {
                "cdno_id": ["CDNO_99"],
                "label": ["glucose"],
                "chebi_id": pd.array([999], dtype="Int64"),
                "fdc_nutrient_ids": [[500]],
            }
        )
        with patch(f"{_IE}.load_cdno", return_value=cdno):
            append_entities_from_cdno(store, _cfg(tmp_path))
        assert store._entities.loc["e1"]["external_ids"]["chebi"] == [999]


class TestAppendFromFdc:
    def test_raises_on_duplicate_fdc_id(self, tmp_path: Path) -> None:
        ents = [
            _chem("e1", "iron", {"fdc_nutrient": [1001]}),
            _chem("e2", "zinc", {"fdc_nutrient": [1001]}),
        ]
        store = _store(tmp_path, ents, {"iron": ["e1"], "zinc": ["e2"]})
        fdc = pd.DataFrame({"id": [1001], "name": ["iron"]}).set_index("id")
        with (
            patch(f"{_IE}.load_fdc_nutrient", return_value=fdc),
            pytest.raises(ValueError, match="Duplicate FDC"),
        ):
            append_entities_from_fdc(store, _cfg(tmp_path))

    def test_skips_non_chemical(self, tmp_path: Path) -> None:
        store = _store(
            tmp_path, [_ent("e1", "food", "apple", ["apple"], {"fdc_nutrient": [1001]})]
        )
        fdc = pd.DataFrame({"id": [1001], "name": ["fiber"]}).set_index("id")
        with patch(f"{_IE}.load_fdc_nutrient", return_value=fdc):
            append_entities_from_fdc(store, _cfg(tmp_path))
        assert len(store._entities) == 2

    def test_links_by_fdc_id_and_name(self, tmp_path: Path) -> None:
        ents = [_chem("e1", "iron", {"fdc_nutrient": [1001]}), _chem("e2", "zinc", {})]
        store = _store(tmp_path, ents, {"iron": ["e1"], "zinc": ["e2"]})
        fdc = pd.DataFrame(
            {"id": [1001, 1002, 9999], "name": ["iron", "zinc", "copper"]}
        ).set_index("id")
        with patch(f"{_IE}.load_fdc_nutrient", return_value=fdc):
            append_entities_from_fdc(store, _cfg(tmp_path))
        assert store._entities.at["e2", "external_ids"]["fdc_nutrient"] == [1002]
        assert len(store._entities) == 3
        assert store._entities.loc["e3"]["common_name"] == "copper"


# ── init_onto ────────────────────────────────────────────────────────


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
