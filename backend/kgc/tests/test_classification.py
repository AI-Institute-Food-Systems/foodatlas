"""Tests for ChEBI-based chemical classification."""

import pandas as pd
import pytest
from src.models.attributes import ChemicalAttributes
from src.pipeline.enrichment.classification import (
    CHEMICAL_CATEGORIES,
    _build_parent_child_map,
    _get_all_descendants,
    classify_chemicals,
)
from src.stores.schema import INDEX_COL

IC = INDEX_COL


def _get_groups(entities: pd.DataFrame, eid: str) -> list[str]:
    """Extract chemical_groups from the attributes dict."""
    raw = entities.at[eid, "attributes"]
    result: list[str] = ChemicalAttributes.model_validate(raw).chemical_groups
    return result


# ── hierarchy helpers ──────────────────────────────────────────────


class TestBuildParentChildMap:
    def test_builds_map(self) -> None:
        triplets = pd.DataFrame(
            [
                {IC: "t0", "head_id": "e1", "tail_id": "e2", "relationship_id": "r2"},
                {IC: "t1", "head_id": "e1", "tail_id": "e3", "relationship_id": "r2"},
                {IC: "t2", "head_id": "e4", "tail_id": "e5", "relationship_id": "r1"},
            ]
        ).set_index(IC)
        result = _build_parent_child_map(triplets, {"e1", "e2", "e3"})
        assert result == {"e1": {"e2", "e3"}}

    def test_filters_to_chem_ids(self) -> None:
        triplets = pd.DataFrame(
            [
                {IC: "t0", "head_id": "e1", "tail_id": "e2", "relationship_id": "r2"},
            ]
        ).set_index(IC)
        result = _build_parent_child_map(triplets, {"e1"})
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


# ── classification ─────────────────────────────────────────────────


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


class TestClassifyChemicals:
    @pytest.fixture()
    def stores(self) -> tuple:
        entities = pd.DataFrame(
            [
                {
                    IC: "e65128",
                    "entity_type": "chemical",
                    "common_name": "flavonoid",
                    "attributes": {},
                },
                {
                    IC: "child1",
                    "entity_type": "chemical",
                    "common_name": "catechin",
                    "attributes": {},
                },
                {
                    IC: "e12451",
                    "entity_type": "chemical",
                    "common_name": "alkaloid",
                    "attributes": {},
                },
                {
                    IC: "child2",
                    "entity_type": "chemical",
                    "common_name": "caffeine",
                    "attributes": {},
                },
                {
                    IC: "lone",
                    "entity_type": "chemical",
                    "common_name": "water",
                    "attributes": {},
                },
                {
                    IC: "f1",
                    "entity_type": "food",
                    "common_name": "apple",
                    "attributes": {},
                },
            ]
        ).set_index(IC)

        triplets = pd.DataFrame(
            [
                {
                    IC: "t0",
                    "head_id": "e65128",
                    "tail_id": "child1",
                    "relationship_id": "r2",
                },
                {
                    IC: "t1",
                    "head_id": "e12451",
                    "tail_id": "child2",
                    "relationship_id": "r2",
                },
            ]
        ).set_index(IC)

        return _make_stores(entities, triplets)

    def test_classifies_anchor(self, stores: tuple) -> None:
        es, ts = stores
        classify_chemicals(es, ts)
        assert "flavonoid" in _get_groups(es._entities, "e65128")

    def test_classifies_descendant(self, stores: tuple) -> None:
        es, ts = stores
        classify_chemicals(es, ts)
        assert "flavonoid" in _get_groups(es._entities, "child1")

    def test_classifies_multiple(self, stores: tuple) -> None:
        es, ts = stores
        classify_chemicals(es, ts)
        assert "alkaloid" in _get_groups(es._entities, "child2")
        assert "alkaloid" in _get_groups(es._entities, "e12451")

    def test_unclassified_empty_list(self, stores: tuple) -> None:
        es, ts = stores
        classify_chemicals(es, ts)
        assert _get_groups(es._entities, "lone") == []

    def test_food_entities_untouched(self, stores: tuple) -> None:
        es, ts = stores
        classify_chemicals(es, ts)
        assert es._entities.at["f1", "attributes"] == {}

    def test_multi_label(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    IC: "e12163",
                    "entity_type": "chemical",
                    "common_name": "polyphenol",
                    "attributes": {},
                },
                {
                    IC: "e13486",
                    "entity_type": "chemical",
                    "common_name": "tannin",
                    "attributes": {},
                },
                {
                    IC: "leaf",
                    "entity_type": "chemical",
                    "common_name": "tannic acid",
                    "attributes": {},
                },
            ]
        ).set_index(IC)

        triplets = pd.DataFrame(
            [
                {
                    IC: "t0",
                    "head_id": "e12163",
                    "tail_id": "e13486",
                    "relationship_id": "r2",
                },
                {
                    IC: "t1",
                    "head_id": "e13486",
                    "tail_id": "leaf",
                    "relationship_id": "r2",
                },
            ]
        ).set_index(IC)

        es, ts = _make_stores(entities, triplets)
        classify_chemicals(es, ts)

        labels = _get_groups(es._entities, "leaf")
        assert "tannin" in labels
        assert "polyphenol" in labels

    def test_preserves_existing_attributes(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    IC: "e65128",
                    "entity_type": "chemical",
                    "common_name": "flavonoid",
                    "attributes": {"flavor_descriptors": ["bitter"]},
                },
            ]
        ).set_index(IC)

        triplets = pd.DataFrame(
            [{IC: "t0", "head_id": "x", "tail_id": "y", "relationship_id": "r2"}]
        ).set_index(IC)

        es, ts = _make_stores(entities, triplets)
        classify_chemicals(es, ts)

        attrs = ChemicalAttributes.model_validate(
            es._entities.at["e65128", "attributes"]
        )
        assert attrs.flavor_descriptors == ["bitter"]
        assert "flavonoid" in attrs.chemical_groups


class TestCategoryConstants:
    def test_no_duplicate_eids(self) -> None:
        eids = list(CHEMICAL_CATEGORIES.values())
        assert len(eids) == len(set(eids))

    def test_twelve_categories(self) -> None:
        assert len(CHEMICAL_CATEGORIES) == 12
