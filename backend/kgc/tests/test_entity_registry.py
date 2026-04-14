"""Tests for EntityRegistry — persistent entity ID mapping."""

from pathlib import Path

import pandas as pd
import pytest
from src.stores.entity_registry import EntityRegistry
from src.stores.schema import REGISTRY_COLUMNS


@pytest.fixture()
def empty_registry(tmp_path: Path) -> EntityRegistry:
    path = tmp_path / "entity_registry.parquet"
    pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(path, index=False)
    return EntityRegistry(path)


@pytest.fixture()
def populated_registry(tmp_path: Path) -> EntityRegistry:
    path = tmp_path / "entity_registry.parquet"
    rows = [
        {"source": "foodon", "native_id": "FOOD_001", "foodatlas_id": "e1"},
        {"source": "chebi", "native_id": "12345", "foodatlas_id": "e2"},
        {"source": "fdc", "native_id": "100", "foodatlas_id": "e1"},
    ]
    pd.DataFrame(rows).to_parquet(path, index=False)
    return EntityRegistry(path)


class TestInit:
    def test_empty_has_next_eid_1(self, empty_registry: EntityRegistry) -> None:
        assert empty_registry.next_eid == 1

    def test_nonexistent_file(self, tmp_path: Path) -> None:
        reg = EntityRegistry(tmp_path / "missing.parquet")
        assert len(reg) == 0
        assert reg.next_eid == 1

    def test_loads_existing(self, populated_registry: EntityRegistry) -> None:
        assert len(populated_registry) == 3
        assert populated_registry.next_eid == 3


class TestResolve:
    def test_known_entity(self, populated_registry: EntityRegistry) -> None:
        assert populated_registry.resolve("foodon", "FOOD_001") == ["e1"]

    def test_unknown_returns_empty_list(
        self, populated_registry: EntityRegistry
    ) -> None:
        assert populated_registry.resolve("foodon", "FOOD_999") == []

    def test_native_id_cast_to_str(self, populated_registry: EntityRegistry) -> None:
        assert populated_registry.resolve("chebi", 12345) == ["e2"]

    def test_alias_resolves(self, populated_registry: EntityRegistry) -> None:
        assert populated_registry.resolve("fdc", "100") == ["e1"]


class TestRegister:
    def test_new_entity(self, empty_registry: EntityRegistry) -> None:
        empty_registry.register("foodon", "FOOD_001", "e1")
        assert empty_registry.resolve("foodon", "FOOD_001") == ["e1"]
        assert empty_registry.next_eid == 2

    def test_updates_max_eid(self, empty_registry: EntityRegistry) -> None:
        empty_registry.register("foodon", "A", "e10")
        empty_registry.register("chebi", "B", "e5")
        assert empty_registry.next_eid == 11

    def test_duplicate_raises(self, populated_registry: EntityRegistry) -> None:
        with pytest.raises(ValueError, match="Duplicate"):
            populated_registry.register("foodon", "FOOD_001", "e99")

    def test_native_id_int_stored_as_str(self, empty_registry: EntityRegistry) -> None:
        empty_registry.register("chebi", 12345, "e1")
        assert empty_registry.resolve("chebi", "12345") == ["e1"]


class TestRegisterAlias:
    def test_new_alias(self, populated_registry: EntityRegistry) -> None:
        populated_registry.register_alias("cdno", "CDNO_1", "e2")
        assert populated_registry.resolve("cdno", "CDNO_1") == ["e2"]

    def test_same_entity_noop(self, populated_registry: EntityRegistry) -> None:
        populated_registry.register_alias("fdc", "100", "e1")
        assert populated_registry.resolve("fdc", "100") == ["e1"]

    def test_appends_second_entity(self, populated_registry: EntityRegistry) -> None:
        populated_registry.register_alias("fdc", "100", "e2")
        result = populated_registry.resolve("fdc", "100")
        assert set(result) == {"e1", "e2"}

    def test_no_duplicates(self, populated_registry: EntityRegistry) -> None:
        populated_registry.register_alias("fdc", "100", "e1")
        populated_registry.register_alias("fdc", "100", "e1")
        assert populated_registry.resolve("fdc", "100") == ["e1"]


class TestReassign:
    def test_reassign_updates_mapping(self, populated_registry: EntityRegistry) -> None:
        old = populated_registry.reassign("chebi", "12345", "e50")
        assert old == "e2"
        assert populated_registry.resolve("chebi", "12345") == ["e50"]

    def test_reassign_updates_max_eid(self, populated_registry: EntityRegistry) -> None:
        populated_registry.reassign("chebi", "12345", "e999")
        assert populated_registry.next_eid == 1000

    def test_reassign_new_key(self, empty_registry: EntityRegistry) -> None:
        old = empty_registry.reassign("chebi", "999", "e5")
        assert old == ""
        assert empty_registry.resolve("chebi", "999") == ["e5"]


class TestAllIds:
    def test_returns_distinct(self, populated_registry: EntityRegistry) -> None:
        ids = populated_registry.all_ids()
        assert ids == {"e1", "e2"}

    def test_includes_1n_ids(self, empty_registry: EntityRegistry) -> None:
        empty_registry.register("chebi", "1", "e1")
        empty_registry.register_alias("chebi", "1", "e2")
        assert empty_registry.all_ids() == {"e1", "e2"}


class TestSaveRoundTrip:
    def test_save_and_reload(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        empty_registry.register("foodon", "F1", "e1")
        empty_registry.register("chebi", "C1", "e2")
        empty_registry.register_alias("fdc", "100", "e1")

        out = tmp_path / "out_registry.parquet"
        empty_registry.save(out)

        reloaded = EntityRegistry(out)
        assert len(reloaded) == 3
        assert reloaded.resolve("foodon", "F1") == ["e1"]
        assert reloaded.resolve("fdc", "100") == ["e1"]
        assert reloaded.next_eid == 3

    def test_1n_round_trip(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        empty_registry.register("chebi", "123", "e1")
        empty_registry.register_alias("chebi", "123", "e2")

        out = tmp_path / "out_registry.parquet"
        empty_registry.save(out)

        reloaded = EntityRegistry(out)
        assert set(reloaded.resolve("chebi", "123")) == {"e1", "e2"}
