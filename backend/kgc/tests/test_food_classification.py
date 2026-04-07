"""Tests for FoodOn-based food classification."""

import pandas as pd
import pytest
from src.models.attributes import FoodAttributes
from src.pipeline.enrichment.food_classification import (
    FOOD_CATEGORIES,
    _build_parent_child_map,
    _get_all_descendants,
    classify_foods,
)
from src.stores.schema import INDEX_COL

IC = INDEX_COL


def _get_groups(entities: pd.DataFrame, eid: str) -> list[str]:
    """Extract food_groups from the attributes dict."""
    raw = entities.at[eid, "attributes"]
    result: list[str] = FoodAttributes.model_validate(raw).food_groups
    return result


# -- hierarchy helpers --------------------------------------------------


class TestBuildParentChildMap:
    def test_builds_map_food_direction(self) -> None:
        """Food IS_A: head=child, tail=parent -> parent maps to child."""
        triplets = pd.DataFrame(
            [
                {
                    IC: "t0",
                    "head_id": "child",
                    "tail_id": "parent",
                    "relationship_id": "r2",
                },
            ]
        ).set_index(IC)
        result = _build_parent_child_map(triplets, {"child", "parent"})
        assert result == {"parent": {"child"}}

    def test_filters_to_food_ids(self) -> None:
        triplets = pd.DataFrame(
            [
                {
                    IC: "t0",
                    "head_id": "food1",
                    "tail_id": "chem1",
                    "relationship_id": "r2",
                },
            ]
        ).set_index(IC)
        result = _build_parent_child_map(triplets, {"food1"})
        assert result == {}

    def test_ignores_non_is_a(self) -> None:
        triplets = pd.DataFrame(
            [
                {
                    IC: "t0",
                    "head_id": "child",
                    "tail_id": "parent",
                    "relationship_id": "r1",
                },
            ]
        ).set_index(IC)
        result = _build_parent_child_map(triplets, {"child", "parent"})
        assert result == {}


class TestGetAllDescendants:
    def test_linear_chain(self) -> None:
        children = {"a": {"b"}, "b": {"c"}}
        assert _get_all_descendants("a", children) == {"b", "c"}

    def test_branching(self) -> None:
        children = {"a": {"b", "c"}, "b": {"d"}}
        assert _get_all_descendants("a", children) == {"b", "c", "d"}

    def test_no_children(self) -> None:
        assert _get_all_descendants("a", {}) == set()

    def test_cycle_safe(self) -> None:
        children = {"a": {"b"}, "b": {"a"}}
        result = _get_all_descendants("a", children)
        assert result == {"a", "b"}


# -- classification -----------------------------------------------------


class _FakeEntityStore:
    _entities: pd.DataFrame


class _FakeTripletStore:
    _triplets: pd.DataFrame


def _make_stores(
    entities: pd.DataFrame, triplets: pd.DataFrame
) -> tuple[_FakeEntityStore, _FakeTripletStore]:
    es = _FakeEntityStore()
    es._entities = entities
    ts = _FakeTripletStore()
    ts._triplets = triplets
    return es, ts


class TestClassifyFoods:
    @pytest.fixture()
    def stores(self) -> tuple:
        """Minimal food hierarchy:

        e19 (plant food product)
          <- e234 (vegetable food product)
              <- leaf1 (carrot)
        e2032 (animal food product)
          <- leaf2 (beef)
        lone (tofu) — no IS_A edges
        chem1 (chemical, should be untouched)
        """
        entities = pd.DataFrame(
            [
                {
                    IC: "e19",
                    "entity_type": "food",
                    "common_name": "plant food product",
                    "attributes": {},
                },
                {
                    IC: "e234",
                    "entity_type": "food",
                    "common_name": "vegetable food product",
                    "attributes": {},
                },
                {
                    IC: "leaf1",
                    "entity_type": "food",
                    "common_name": "carrot",
                    "attributes": {},
                },
                {
                    IC: "e2032",
                    "entity_type": "food",
                    "common_name": "animal food product",
                    "attributes": {},
                },
                {
                    IC: "leaf2",
                    "entity_type": "food",
                    "common_name": "beef",
                    "attributes": {},
                },
                {
                    IC: "lone",
                    "entity_type": "food",
                    "common_name": "tofu",
                    "attributes": {},
                },
                {
                    IC: "chem1",
                    "entity_type": "chemical",
                    "common_name": "water",
                    "attributes": {},
                },
            ]
        ).set_index(IC)

        # Food IS_A: head=child, tail=parent
        triplets = pd.DataFrame(
            [
                {
                    IC: "t0",
                    "head_id": "e234",
                    "tail_id": "e19",
                    "relationship_id": "r2",
                },
                {
                    IC: "t1",
                    "head_id": "leaf1",
                    "tail_id": "e234",
                    "relationship_id": "r2",
                },
                {
                    IC: "t2",
                    "head_id": "leaf2",
                    "tail_id": "e2032",
                    "relationship_id": "r2",
                },
            ]
        ).set_index(IC)

        return _make_stores(entities, triplets)

    def test_classifies_anchor(self, stores: tuple) -> None:
        es, ts = stores
        classify_foods(es, ts)
        groups = _get_groups(es._entities, "e19")
        assert "plant food product" in groups

    def test_classifies_descendant_gets_broad_label(self, stores: tuple) -> None:
        es, ts = stores
        classify_foods(es, ts)
        groups = _get_groups(es._entities, "leaf1")
        assert "plant food product" in groups

    def test_animal_descendant(self, stores: tuple) -> None:
        es, ts = stores
        classify_foods(es, ts)
        groups = _get_groups(es._entities, "leaf2")
        assert "animal food product" in groups

    def test_unclassified_empty_list(self, stores: tuple) -> None:
        es, ts = stores
        classify_foods(es, ts)
        assert _get_groups(es._entities, "lone") == []

    def test_chemical_untouched(self, stores: tuple) -> None:
        es, ts = stores
        classify_foods(es, ts)
        assert es._entities.at["chem1", "attributes"] == {}

    def test_multi_label(self) -> None:
        """A food that is a descendant of two specific anchors."""
        entities = pd.DataFrame(
            [
                {
                    IC: "e19",
                    "entity_type": "food",
                    "common_name": "plant food product",
                    "attributes": {},
                },
                {
                    IC: "e59",
                    "entity_type": "food",
                    "common_name": "plant fruit food product",
                    "attributes": {},
                },
                {
                    IC: "e10145",
                    "entity_type": "food",
                    "common_name": "plant seed or nut food product",
                    "attributes": {},
                },
                {
                    IC: "leaf",
                    "entity_type": "food",
                    "common_name": "tomato seed",
                    "attributes": {},
                },
            ]
        ).set_index(IC)

        triplets = pd.DataFrame(
            [
                {
                    IC: "t0",
                    "head_id": "e59",
                    "tail_id": "e19",
                    "relationship_id": "r2",
                },
                {
                    IC: "t1",
                    "head_id": "e10145",
                    "tail_id": "e19",
                    "relationship_id": "r2",
                },
                {
                    IC: "t2",
                    "head_id": "leaf",
                    "tail_id": "e59",
                    "relationship_id": "r2",
                },
                {
                    IC: "t3",
                    "head_id": "leaf",
                    "tail_id": "e10145",
                    "relationship_id": "r2",
                },
            ]
        ).set_index(IC)

        es, ts = _make_stores(entities, triplets)
        classify_foods(es, ts)

        groups = _get_groups(es._entities, "leaf")
        assert "plant food product" in groups
        assert "plant fruit food product" in groups
        assert "plant seed or nut food product" in groups

    def test_preserves_existing_attributes(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    IC: "e19",
                    "entity_type": "food",
                    "common_name": "plant food product",
                    "attributes": {"food_groups": ["stale"]},
                },
            ]
        ).set_index(IC)

        triplets = pd.DataFrame(
            [{IC: "t0", "head_id": "x", "tail_id": "y", "relationship_id": "r2"}]
        ).set_index(IC)

        es, ts = _make_stores(entities, triplets)
        classify_foods(es, ts)

        attrs = FoodAttributes.model_validate(es._entities.at["e19", "attributes"])
        assert "plant food product" in attrs.food_groups


class TestFoodCategoryConstants:
    def test_no_duplicate_eids(self) -> None:
        eids = list(FOOD_CATEGORIES.values())
        assert len(eids) == len(set(eids))

    def test_thirteen_categories(self) -> None:
        assert len(FOOD_CATEGORIES) == 13
