"""Tests for registry_diff — build diff computation and reporting."""

import json
from pathlib import Path

from src.stores.registry_diff import (
    RegistryDiff,
    build_retired_df,
    compute_diff,
    write_diff_report,
)


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


class TestBuildRetiredDf:
    def test_empty_diff(self) -> None:
        df = build_retired_df(RegistryDiff())
        assert len(df) == 0
        assert list(df.columns) == ["foodatlas_id", "action", "destination"]

    def test_retired_and_merged(self) -> None:
        diff = RegistryDiff(
            retired_ids=["e3"],
            merged=[("e2", "e1")],
        )
        df = build_retired_df(diff)
        assert len(df) == 2
        retired_row = df[df["foodatlas_id"] == "e3"].iloc[0]
        assert retired_row["action"] == "retired"
        assert retired_row["destination"] == ""
        merged_row = df[df["foodatlas_id"] == "e2"].iloc[0]
        assert merged_row["action"] == "merged"
        assert merged_row["destination"] == "e1"


class TestWriteDiffReport:
    def test_writes_json(self, tmp_path: Path) -> None:
        diff = RegistryDiff(
            new_ids=["e5"],
            retired_ids=["e4"],
            merged=[("e3", "e1")],
            stable_ids=["e1", "e2"],
        )
        write_diff_report(diff, tmp_path)
        path = tmp_path / "reports" / "build_diff.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["new_count"] == 1
        assert data["retired_count"] == 1
        assert data["merged_count"] == 1
        assert data["stable_count"] == 2
