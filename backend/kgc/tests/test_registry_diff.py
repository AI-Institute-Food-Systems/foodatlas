"""Tests for registry_diff — build diff computation."""

from src.stores.registry_diff import RegistryDiff, compute_diff


class TestComputeDiff:
    def test_all_stable(self) -> None:
        diff = compute_diff({"e1", "e2"}, {"e1", "e2"}, {})
        assert diff.new_ids == []
        assert diff.retired_ids == []
        assert diff.merged == []
        assert sorted(diff.stable_ids) == ["e1", "e2"]

    def test_new_entities(self) -> None:
        diff = compute_diff({"e1"}, {"e1", "e2", "e3"}, {})
        assert diff.new_ids == ["e2", "e3"]
        assert diff.stable_ids == ["e1"]

    def test_retired_entities(self) -> None:
        diff = compute_diff({"e1", "e2", "e3"}, {"e1"}, {})
        assert diff.retired_ids == ["e2", "e3"]
        assert diff.stable_ids == ["e1"]

    def test_merged_entities(self) -> None:
        diff = compute_diff({"e1", "e2"}, {"e1"}, {"e2": "e1"})
        assert diff.retired_ids == []
        assert diff.merged == [("e2", "e1")]
        assert diff.stable_ids == ["e1"]

    def test_mixed(self) -> None:
        old = {"e1", "e2", "e3", "e4"}
        new = {"e1", "e2", "e5"}
        merges = {"e3": "e1"}
        diff = compute_diff(old, new, merges)
        assert diff.new_ids == ["e5"]
        assert diff.retired_ids == ["e4"]
        assert diff.merged == [("e3", "e1")]
        assert sorted(diff.stable_ids) == ["e1", "e2"]

    def test_empty_registries(self) -> None:
        diff = compute_diff(set(), set(), {})
        assert diff == RegistryDiff()

    def test_first_build(self) -> None:
        diff = compute_diff(set(), {"e1", "e2"}, {})
        assert diff.new_ids == ["e1", "e2"]
        assert diff.retired_ids == []
