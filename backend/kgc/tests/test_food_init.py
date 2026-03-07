"""Tests for food initialization: loaders, init_entities (FDC), init_onto."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from src.integration.entities.food.init_entities import (
    _rebuild_food_lut,
    append_foods_from_fdc,
)
from src.integration.entities.food.loaders import (
    _extract_foodon_url,
    _resolve_multiple_foodon_urls,
    _resolve_organisms,
    _resolve_singular_plural,
    load_fdc,
    load_foodon,
    load_lut_food,
)
from src.integration.ontologies.food import (
    _build_foodon_to_fa_map,
    _traverse_hierarchy,
    create_food_ontology,
)
from src.models.settings import KGCSettings
from src.stores.entity_store import EntityStore
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_FOOD_ONTOLOGY,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)

FA = "http://purl.obolibrary.org/obo/FOODON_001"
FB = "http://purl.obolibrary.org/obo/FOODON_002"


def _wj(path: Path, data: object) -> None:
    with path.open("w") as f:
        json.dump(data, f)


def _ent(eid: str, name: str, ext: dict, etype: str = "food") -> dict:
    return {
        "foodatlas_id": eid,
        "entity_type": etype,
        "common_name": name,
        "scientific_name": None,
        "synonyms": [name],
        "external_ids": ext,
        "_synonyms_display": {},
    }


def _store(tmp: Path, ents: list | None = None, lf: dict | None = None) -> EntityStore:
    _wj(tmp / FILE_ENTITIES, ents or [])
    _wj(tmp / FILE_LUT_FOOD, lf or {})
    _wj(tmp / FILE_LUT_CHEMICAL, {})
    return EntityStore(
        path_entities=tmp / FILE_ENTITIES,
        path_lut_food=tmp / FILE_LUT_FOOD,
        path_lut_chemical=tmp / FILE_LUT_CHEMICAL,
    )


def _settings(tmp: Path) -> KGCSettings:
    return KGCSettings(kg_dir=str(tmp), data_dir=str(tmp), integration_dir=str(tmp))


def _syns(label: list[str] | None = None) -> dict:
    base: dict[str, list[str]] = {
        k: []
        for k in [
            "label",
            "label (alternative)",
            "synonym (exact)",
            "synonym",
            "synonym (narrow)",
            "synonym (broad)",
        ]
    }
    if label:
        base["label"] = label
    return base


def _fon(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).set_index("foodon_id")


def _fon_row(
    fid: str,
    label: str,
    *,
    food: bool = True,
    parents: list | None = None,
    derives: list | None = None,
    has_part: list | None = None,
) -> dict:
    return {
        "foodon_id": fid,
        "is_food": food,
        "is_organism": not food,
        "parents": parents or [],
        "synonyms": _syns([label]),
        "derives": derives or [],
        "has_part": has_part or [],
        "derives_from": [],
        "in_taxon": [],
    }


@pytest.fixture()
def foodon2() -> pd.DataFrame:
    return _fon([_fon_row(FA, "apple", parents=[FB]), _fon_row(FB, "banana")])


# ── loaders ──────────────────────────────────────────────────────────


class TestLoadFoodon:
    def test_reads_parquet(self, tmp_path: Path, foodon2: pd.DataFrame) -> None:
        with patch("src.integration.entities.food.loaders.pd.read_parquet") as m:
            m.return_value = foodon2.reset_index()
            r = load_foodon(_settings(tmp_path))
        assert r.index.name == "foodon_id" and len(r) == 2


class TestLoadLutFood:
    def test_builds_lut_no_extras(self, tmp_path: Path, foodon2: pd.DataFrame) -> None:
        with patch(
            "src.integration.entities.food.loaders.load_foodon", return_value=foodon2
        ):
            lut = load_lut_food(
                _settings(tmp_path),
                resolve_organisms=False,
                resolve_singular_plural_forms=False,
            )
        assert lut["apple"] == FA and lut["banana"] == FB

    def test_with_extras(self, tmp_path: Path, foodon2: pd.DataFrame) -> None:
        with patch(
            "src.integration.entities.food.loaders.load_foodon", return_value=foodon2
        ):
            lut = load_lut_food(_settings(tmp_path))
        assert "apples" in lut and "bananas" in lut


class TestResolveOrganisms:
    def test_adds_organism(self) -> None:
        foodon = _fon(
            [
                _fon_row(FA, "apple"),
                _fon_row("ORG", "apple tree", food=False, derives=[FA]),
            ]
        )
        lut: dict[str, str] = {"apple": FA}
        _resolve_organisms(foodon, lut, ["label"])
        assert lut["apple tree"] == FA

    def test_skips_multi(self) -> None:
        foodon = _fon([_fon_row("ORG", "x", food=False, derives=[FA, FB])])
        lut: dict[str, str] = {}
        _resolve_organisms(foodon, lut, ["label"])
        assert "x" not in lut


class TestResolveSingularPlural:
    def test_adds_plural(self) -> None:
        lut: dict[str, str] = {"apple": "F1"}
        _resolve_singular_plural(lut)
        assert "apples" in lut

    def test_skips_non_alpha(self) -> None:
        lut: dict[str, str] = {"item-": "F1"}
        _resolve_singular_plural(lut)
        assert len(lut) == 1


class TestLoadFdc:
    def test_reads_csvs(self, tmp_path: Path) -> None:
        d = tmp_path / "FDC" / "FoodData_Central_foundation_food_csv_2024-04-18"
        d.mkdir(parents=True)
        pd.DataFrame({"fdc_id": [100]}).to_csv(d / "foundation_food.csv", index=False)
        pd.DataFrame({"fdc_id": [100], "description": [" Apple "]}).to_csv(
            d / "food.csv", index=False
        )
        pd.DataFrame(
            {
                "fdc_id": [100],
                "name": ["FoodOn Ontology ID for FDC Item"],
                "value": [FA],
            }
        ).to_csv(d / "food_attribute.csv", index=False)
        r = load_fdc(_settings(tmp_path))
        assert r.loc[100, "description"] == "apple" and r.loc[100, "foodon_url"] == FA


class TestExtractFoodonUrl:
    def test_normal(self) -> None:
        row = pd.Series({"description": "apple"}, name=100)
        fa = pd.DataFrame(
            {"name": ["FoodOn Ontology ID for FDC Item"], "value": [FA]},
            index=pd.Index([100], name="fdc_id"),
        )
        assert _extract_foodon_url(row, fa) == FA

    def test_hardcoded_fallback(self) -> None:
        row = pd.Series({"description": "t"}, name=2512381)
        fa = pd.DataFrame(columns=["name", "value"])
        fa.index.name = "fdc_id"
        assert "FOODON_03000273" in _extract_foodon_url(row, fa)

    def test_missing_raises(self) -> None:
        row = pd.Series({"description": "x"}, name=999)
        fa = pd.DataFrame(columns=["name", "value"])
        fa.index.name = "fdc_id"
        with pytest.raises(ValueError, match="without FoodOn ID"):
            _extract_foodon_url(row, fa)

    def test_no_attr_raises(self) -> None:
        row = pd.Series({"description": "x"}, name=100)
        fa = pd.DataFrame(
            {"name": ["other"], "value": ["v"]},
            index=pd.Index([100], name="fdc_id"),
        )
        with pytest.raises(ValueError, match="without FoodOn ID"):
            _extract_foodon_url(row, fa)


class TestResolveMultipleFoodonUrls:
    def test_known_fix(self) -> None:
        assert "FOODON_03310577" in _resolve_multiple_foodon_urls(323121, ["a", "b"])

    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unaddressed"):
            _resolve_multiple_foodon_urls(999999, ["a", "b"])


# ── init_entities (FDC) ──────────────────────────────────────────────


class TestAppendFoodsFromFdc:
    def _base_store(self, tmp: Path) -> EntityStore:
        return _store(tmp, [_ent("e1", "apple", {"foodon": [FA]})], {"apple": ["e1"]})

    def test_links_and_creates(self, tmp_path: Path) -> None:
        store = self._base_store(tmp_path)
        fdc = pd.DataFrame(
            [
                {"description": "apple", "foodon_url": FA},
                {"description": "carrot", "foodon_url": "UNKNOWN"},
            ],
            index=pd.Index([100, 200], name="fdc_id"),
        )
        with patch(
            "src.integration.entities.food.init_entities.load_fdc", return_value=fdc
        ):
            append_foods_from_fdc(store, _settings(tmp_path))
        assert 100 in store._entities.at["e1", "external_ids"]["fdc"]
        assert store._entities.iloc[1]["common_name"] == "carrot"
        assert "carrot" in store._lut_food

    def test_duplicate_foodon_raises(self, tmp_path: Path) -> None:
        ents = [_ent("e1", "a", {"foodon": [FA]}), _ent("e2", "b", {"foodon": [FA]})]
        store = _store(tmp_path, ents)
        fdc = pd.DataFrame(columns=["description", "foodon_url"])
        with (
            patch(
                "src.integration.entities.food.init_entities.load_fdc", return_value=fdc
            ),
            pytest.raises(ValueError, match="Duplicate"),
        ):
            append_foods_from_fdc(store, _settings(tmp_path))


class TestRebuildFoodLut:
    def test_maps_names(self, tmp_path: Path) -> None:
        store = _store(tmp_path)
        ents_new = pd.DataFrame(
            [{"external_ids": {"foodon": [FA]}}],
            index=pd.Index(["e1"], name="foodatlas_id"),
        )
        lut_df = pd.DataFrame(
            [
                {"name": "apple", "foodon_id": FA},
                {"name": "apples", "foodon_id": FA},
            ]
        )
        _rebuild_food_lut(store, ents_new, lut_df)
        assert store._lut_food["apple"] == ["e1"]
        assert store._lut_food["apples"] == ["e1"]


# ── init_onto ────────────────────────────────────────────────────────


class TestBuildFoodonToFaMap:
    def test_maps(self, tmp_path: Path) -> None:
        store = _store(tmp_path, [_ent("e1", "apple", {"foodon": [FA]})])
        assert _build_foodon_to_fa_map(store) == {FA: "e1"}

    def test_skips_non_foodon(self, tmp_path: Path) -> None:
        store = _store(tmp_path, [_ent("e1", "vc", {"pubchem": [1]}, "chemical")])
        assert _build_foodon_to_fa_map(store) == {}


class TestTraverseHierarchy:
    def test_collects_is_a(self, foodon2: pd.DataFrame) -> None:
        rows = _traverse_hierarchy(foodon2, {FA: "e1", FB: "e2"})
        assert len(rows) == 1
        assert rows[0] == {
            "foodatlas_id": None,
            "head_id": "e1",
            "relationship_id": "r2",
            "tail_id": "e2",
            "source": "foodon",
        }

    def test_no_parents(self) -> None:
        foodon = _fon([_fon_row(FA, "apple")])
        assert _traverse_hierarchy(foodon, {FA: "e1"}) == []


class TestCreateFoodOntology:
    def test_end_to_end(self, tmp_path: Path, foodon2: pd.DataFrame) -> None:
        ents = [
            _ent("e1", "apple", {"foodon": [FA]}),
            _ent("e2", "banana", {"foodon": [FB]}),
        ]
        store = _store(tmp_path, ents)
        with patch("src.integration.ontologies.food.load_foodon", return_value=foodon2):
            result = create_food_ontology(store, _settings(tmp_path))
        assert len(result) == 1
        assert (tmp_path / FILE_FOOD_ONTOLOGY).exists()
        with (tmp_path / FILE_FOOD_ONTOLOGY).open() as f:
            saved = json.load(f)
        assert saved[0]["head_id"] == "e1" and saved[0]["tail_id"] == "e2"
