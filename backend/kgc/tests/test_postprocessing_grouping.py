"""Tests for postprocessing grouping modules: chemicals, foods, mesh."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.models.settings import KGCSettings
from src.postprocessing.grouping.chemicals import (
    _assign_label,
    _build_chebi_to_group,
    _build_is_a_map,
    _get_group_eids_at_level,
    _invert_is_a,
    _map_entities_to_groups,
    _traverse_cdno_hierarchy,
    generate_chemical_groups_cdno,
)
from src.postprocessing.grouping.foods import (
    _build_group_mapping,
    _resolve_group_eids,
    _traverse_hierarchy,
)
from src.postprocessing.grouping.mesh import (
    _add_tree_levels,
    _assign_categories,
    _build_tree_number_to_category,
    _filter_chemical_trees,
    assign_mesh_groups,
)
from src.stores.entity_store import EntityStore
from src.stores.schema import FILE_ENTITIES, FILE_LUT_CHEMICAL, FILE_LUT_FOOD, INDEX_COL

IC = INDEX_COL  # shorthand for readability


# ── CDNO chemical grouping ──────────────────────────────────────────


class TestTraverseCdnoHierarchy:
    def test_propagates_labels(self) -> None:
        cdno = pd.DataFrame(
            [{"id": "root", "parents": []}, {"id": "child", "parents": ["root"]}]
        ).set_index("id")
        id2p: dict[str, list[str]] = {"root": ["vitamin"]}
        _traverse_cdno_hierarchy(cdno, id2p)
        assert id2p["child"] == ["vitamin"]

    def test_unknown_parent_returns_empty(self) -> None:
        cdno = pd.DataFrame([{"id": "orphan", "parents": ["missing"]}]).set_index("id")
        id2p: dict[str, list[str]] = {}
        _traverse_cdno_hierarchy(cdno, id2p)
        assert id2p["orphan"] == []


class TestBuildChebiToGroup:
    def test_maps_chebi_int(self) -> None:
        cdno = pd.DataFrame(
            [{"id": "A", "chebi_id": "http://obo/CHEBI_123"}]
        ).set_index("id")
        assert _build_chebi_to_group(cdno, {"A": ["vitamin"]})[123] == ["vitamin"]

    def test_skips_no_chebi(self) -> None:
        cdno = pd.DataFrame([{"id": "A", "chebi_id": None}]).set_index("id")
        assert _build_chebi_to_group(cdno, {"A": ["lipid"]}) == {}


class TestMapEntitiesToGroups:
    def test_maps_via_chebi(self) -> None:
        chems = pd.DataFrame([{IC: "e1", "external_ids": {"chebi": [42]}}]).set_index(
            IC
        )
        assert _map_entities_to_groups(chems, {42: ["lipid"]}) == {"e1": ["lipid"]}

    def test_skips_no_chebi(self) -> None:
        chems = pd.DataFrame([{IC: "e1", "external_ids": {}}]).set_index(IC)
        assert _map_entities_to_groups(chems, {42: ["l"]}) == {}


class TestAssignLabel:
    def test_known(self) -> None:
        assert _assign_label("e1", {"e1": ["amino acid"]}) == ["amino acid and protein"]

    def test_unknown(self) -> None:
        assert _assign_label("e1", {"e1": ["water"]}) == ["others"]

    def test_missing(self) -> None:
        assert _assign_label("e99", {}) == ["others"]


class TestGenerateChemicalGroupsCdno:
    def test_end_to_end(self, tmp_path: Path) -> None:
        root_url = "http://purl.obolibrary.org/obo/CDNO_0200179"
        cdno = pd.DataFrame(
            [
                {
                    "id": root_url,
                    "label": "vitamin",
                    "parents": [],
                    "fdc_nutrient_ids": ["1"],
                    "chebi_ids": [],
                },
                {
                    "id": root_url.replace("0200179", "CHILD"),
                    "label": "vitamin A",
                    "parents": [root_url],
                    "fdc_nutrient_ids": ["2"],
                    "chebi_ids": ["http://purl.obolibrary.org/obo/CHEBI_100"],
                },
            ]
        ).set_index("id")
        dp_dir = tmp_path / "integration"
        dp_dir.mkdir()
        cdno.to_parquet(dp_dir / "cdno_hierarchy.parquet")
        chems = pd.DataFrame(
            [
                {IC: "e1", "external_ids": {"chebi": [100]}},
                {IC: "e2", "external_ids": {}},
            ]
        ).set_index(IC)
        result = generate_chemical_groups_cdno(
            chems,
            KGCSettings(
                pipeline={
                    "stages": {
                        "integration": {"data_cleaning": {"output_dir": str(dp_dir)}}
                    }
                }
            ),
        )
        assert result["e1"] == ["vitamin"]
        assert result["e2"] == ["others"]


# ── ChEBI hierarchy helpers ─────────────────────────────────────────


class TestIsAMapHelpers:
    def test_build_and_invert(self) -> None:
        ont = pd.DataFrame(
            [{"head_id": "e1", "tail_id": "e2"}, {"head_id": "e1", "tail_id": "e3"}]
        )
        ht = _build_is_a_map(ont)
        assert ht == {"e1": ["e2", "e3"]}
        inv = _invert_is_a(ht)
        assert "e1" in inv["e2"] and "e1" in inv["e3"]


class TestGetGroupEidsAtLevel:
    def test_level_1(self) -> None:
        assert set(
            _get_group_eids_at_level("root", {"root": {"a", "b"}, "a": {"c"}}, 1)
        ) == {"a", "b"}

    def test_leaf_stays(self) -> None:
        ht: dict[str, set[str]] = {"root": {"leaf"}}
        assert _get_group_eids_at_level("root", ht, level=2) == ["leaf"]


# ── Food grouping ───────────────────────────────────────────────────


class TestResolveGroupEids:
    def test_resolves_known(self, tmp_path: Path) -> None:
        entities = [
            {
                IC: "e0",
                "entity_type": "food",
                "common_name": "dairy food product",
                "synonyms": ["dairy food product"],
                "external_ids": {},
                "_synonyms_display": [],
                "scientific_name": "",
            }
        ]
        with (tmp_path / FILE_ENTITIES).open("w") as f:
            json.dump(entities, f)
        with (tmp_path / FILE_LUT_FOOD).open("w") as f:
            json.dump({"dairy food product": ["e0"]}, f)
        with (tmp_path / FILE_LUT_CHEMICAL).open("w") as f:
            json.dump({}, f)
        store = EntityStore(
            tmp_path / FILE_ENTITIES,
            tmp_path / FILE_LUT_FOOD,
            tmp_path / FILE_LUT_CHEMICAL,
        )
        assert _resolve_group_eids(store, {"dairy food product": "dairy"}) == ["e0"]


class TestBuildGroupMapping:
    def test_level_1(self) -> None:
        foods = pd.DataFrame(
            [{IC: "e0", "common_name": "dairy food product"}]
        ).set_index(IC)
        ht = _build_group_mapping(["e0"], {"dairy food product": "dairy"}, {}, foods, 1)
        assert ht == {"e0": ["dairy"]}

    def test_invalid_level_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid level"):
            _build_group_mapping([], {}, {}, pd.DataFrame(), 3)


class TestTraverseHierarchy:
    def test_dfs_propagation(self) -> None:
        foods = pd.DataFrame([{IC: "e0"}, {IC: "e1"}]).set_index(IC)
        ht: dict[str, list[str]] = {"e1": ["fruit"]}
        _traverse_hierarchy(foods, ht, {"e0": ["e1"]})
        assert ht["e0"] == ["fruit"]


# ── MeSH grouping ───────────────────────────────────────────────────


class TestFilterChemicalTrees:
    def test_keeps_d_prefix(self) -> None:
        df = pd.DataFrame(
            [{"tree_numbers": ["D01.001", "C01.002"]}, {"tree_numbers": ["C02.003"]}]
        )
        result = _filter_chemical_trees(df)
        assert len(result) == 1
        assert result.iloc[0]["tree_numbers"] == ["D01.001"]


class TestAddTreeLevels:
    def test_splits_levels(self) -> None:
        result = _add_tree_levels(pd.DataFrame([{"tree_numbers": ["D01.002.003"]}]))
        assert result.iloc[0]["primary_tree_numbers"] == ["D01"]
        assert result.iloc[0]["secondary_tree_numbers"] == ["D01.002"]


class TestBuildTreeNumberToCategory:
    def test_maps_tree_to_name(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "id": "D001",
                    "name": "Chemicals",
                    "tree_numbers": ["D01"],
                    "primary_tree_numbers": ["D01"],
                    "secondary_tree_numbers": [],
                }
            ]
        )
        assert "D01" in _build_tree_number_to_category(df).index


class TestAssignCategories:
    def test_assigns_names(self) -> None:
        mesh = pd.DataFrame(
            [{"primary_tree_numbers": ["D01"], "secondary_tree_numbers": []}]
        )
        tree_map = pd.DataFrame(
            [{"id": "D001", "name": "Chemicals"}],
            index=pd.Index(["D01"], name="tree_numbers"),
        )
        result = _assign_categories(mesh, tree_map)
        assert result.iloc[0]["primary_category"] == ["Chemicals"]
        assert result.iloc[0]["secondary_category"] == []


class TestAssignMeshGroups:
    def test_assigns_categories(self) -> None:
        chems = pd.DataFrame(
            [
                {IC: "e1", "external_ids": {"mesh": ["D002110"]}},
                {IC: "e2", "external_ids": {}},
            ]
        ).set_index(IC)
        mesh = pd.DataFrame(
            [
                {
                    "id": "D002110",
                    "primary_category": ["Chemicals"],
                    "secondary_category": ["Alkaloids"],
                }
            ]
        )
        result = assign_mesh_groups(chems, mesh)
        assert result.loc["e1", "mesh_lvl1"] == ["Chemicals"]
        assert result.loc["e1", "mesh_lvl2"] == ["Alkaloids"]
        assert result.loc["e2", "mesh_lvl1"] == []
