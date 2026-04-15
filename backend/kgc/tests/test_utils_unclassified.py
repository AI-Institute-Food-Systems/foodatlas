"""Tests for utils.unclassified — entities without IS_A parents."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
from src.utils.unclassified import find_unclassified, write_unclassified_jsonl

if TYPE_CHECKING:
    from pathlib import Path


def _ents(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["foodatlas_id", "entity_type", "common_name"])
    return df.set_index("foodatlas_id")


def _trips(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["head_id", "relationship_id", "tail_id"])


class TestFindUnclassified:
    def test_chemical_with_parent_is_classified(self) -> None:
        # ChEBI convention: head=parent, tail=child.
        # c1 (child) has parent c2 -> c1 is classified, c2 is not.
        ents = _ents([("c1", "chemical", "water"), ("c2", "chemical", "compound")])
        trips = _trips([("c2", "r2", "c1")])
        result = find_unclassified(ents, trips)
        assert list(result.index) == ["c2"]

    def test_food_with_parent_is_classified(self) -> None:
        # FoodOn convention: head=child, tail=parent.
        # f1 (child) has parent f2 -> f1 is classified, f2 is not.
        ents = _ents([("f1", "food", "apple"), ("f2", "food", "fruit")])
        trips = _trips([("f1", "r2", "f2")])
        result = find_unclassified(ents, trips)
        assert list(result.index) == ["f2"]

    def test_ignores_non_food_non_chemical(self) -> None:
        ents = _ents([("d1", "disease", "flu")])
        trips = _trips([])
        result = find_unclassified(ents, trips)
        assert list(result.index) == []

    def test_ignores_non_is_a_edges(self) -> None:
        # c1 has a CONTAINS edge but no IS_A edge
        ents = _ents([("c1", "chemical", "x")])
        trips = _trips([("food1", "r1", "c1")])
        result = find_unclassified(ents, trips)
        assert list(result.index) == ["c1"]

    def test_empty_triplets(self) -> None:
        ents = _ents([("c1", "chemical", "x"), ("f1", "food", "y")])
        trips = _trips([])
        assert set(find_unclassified(ents, trips).index) == {"c1", "f1"}


class TestWriteUnclassifiedJsonl:
    def test_writes_jsonl(self, tmp_path: Path) -> None:
        # c1 is a chemical under parent c2 (ChEBI: head=parent);
        # f1 is a food under parent f2 (FoodOn: head=child).
        # Unclassified: c2 (no parent) and f2 (no parent).
        ents = _ents(
            [
                ("c1", "chemical", "water"),
                ("c2", "chemical", "compound"),
                ("f1", "food", "apple"),
                ("f2", "food", "fruit"),
            ]
        )
        trips = _trips([("c2", "r2", "c1"), ("f1", "r2", "f2")])
        out = tmp_path / "diagnostics" / "kgc_unclassified.jsonl"
        count = write_unclassified_jsonl(ents, trips, out)
        assert count == 2

        records = [json.loads(line) for line in out.read_text().splitlines()]
        ids = {r["foodatlas_id"] for r in records}
        assert ids == {"c2", "f2"}
