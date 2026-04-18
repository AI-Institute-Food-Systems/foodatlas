"""Tests for utils.orphans — orphan entity detection and export."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
from src.utils.orphans import find_orphans, orphan_counts_by_type, write_orphans_jsonl

if TYPE_CHECKING:
    from pathlib import Path


def _ents(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["foodatlas_id", "entity_type", "common_name"])
    return df.set_index("foodatlas_id")


def _trips(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["head_id", "relationship_id", "tail_id"])


class TestFindOrphans:
    def test_head_and_tail_both_referenced(self) -> None:
        ents = _ents(
            [("e1", "food", "a"), ("e2", "chemical", "b"), ("e3", "disease", "c")]
        )
        trips = _trips([("e1", "r1", "e2")])
        orphans = find_orphans(ents, trips)
        assert list(orphans.index) == ["e3"]

    def test_no_triplets(self) -> None:
        ents = _ents([("e1", "food", "a")])
        trips = _trips([])
        assert list(find_orphans(ents, trips).index) == ["e1"]


class TestOrphanCountsByType:
    def test_groups_by_type(self) -> None:
        ents = _ents(
            [
                ("e1", "food", "a"),
                ("e2", "chemical", "b"),
                ("e3", "chemical", "c"),
                ("e4", "disease", "d"),
            ]
        )
        trips = _trips([("e1", "r1", "e2")])
        assert orphan_counts_by_type(ents, trips) == {"chemical": 1, "disease": 1}


class TestWriteOrphansJsonl:
    def test_writes_jsonl(self, tmp_path: Path) -> None:
        ents = _ents(
            [
                ("e1", "food", "apple"),
                ("e2", "chemical", "water"),
                ("e3", "disease", "flu"),
            ]
        )
        trips = _trips([("e1", "r1", "e2")])
        out = tmp_path / "diagnostics" / "kgc_orphans.jsonl"
        count = write_orphans_jsonl(ents, trips, out)
        assert count == 1
        lines = out.read_text().strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record == {
            "foodatlas_id": "e3",
            "entity_type": "disease",
            "common_name": "flu",
        }

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        ents = _ents([("e1", "food", "a")])
        trips = _trips([])
        out = tmp_path / "nested" / "dir" / "orphans.jsonl"
        count = write_orphans_jsonl(ents, trips, out)
        assert count == 1
        assert out.exists()
