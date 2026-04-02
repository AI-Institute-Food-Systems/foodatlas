"""Tests for ie_report — unresolved names report and stats output."""

import json
from pathlib import Path

import pandas as pd
from src.pipeline.ie.report import (
    write_resolution_stats,
    write_unresolved_report,
)


class TestWriteUnresolvedReport:
    @staticmethod
    def _make_metadata() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "_food_name": "banana",
                    "_chemical_name": "x",
                    "reference": ["pmcid:1"],
                },
                {
                    "_food_name": "banana",
                    "_chemical_name": "x",
                    "reference": ["pmcid:2"],
                },
                {
                    "_food_name": "apple",
                    "_chemical_name": "zinc",
                    "reference": ["pmcid:3"],
                },
            ]
        )

    def test_writes_tsv(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        out = write_unresolved_report({"banana"}, {"zinc"}, meta, tmp_path)
        assert out.exists()
        df = pd.read_csv(out, sep="\t")
        assert len(df) == 2
        assert set(df["entity_type"]) == {"food", "chemical"}

    def test_sorted_by_occurrence(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        out = write_unresolved_report({"banana"}, {"zinc"}, meta, tmp_path)
        df = pd.read_csv(out, sep="\t")
        # banana appears 2 times, zinc 1 time — banana should be first.
        assert df.iloc[0]["name"] == "banana"
        assert df.iloc[0]["occurrence_count"] == 2

    def test_empty_unresolved(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        out = write_unresolved_report(set(), set(), meta, tmp_path)
        df = pd.read_csv(out, sep="\t")
        assert len(df) == 0

    def test_columns(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        out = write_unresolved_report({"banana"}, set(), meta, tmp_path)
        df = pd.read_csv(out, sep="\t")
        assert list(df.columns) == [
            "name",
            "entity_type",
            "occurrence_count",
            "sample_references",
        ]


class TestWriteResolutionStats:
    def test_writes_json(self, tmp_path: Path) -> None:
        stats = {"total_ie_rows": 100, "resolved_rows": 80}
        out = write_resolution_stats(stats, tmp_path)
        assert out.exists()
        loaded = json.loads(out.read_text())
        assert loaded["total_ie_rows"] == 100
        assert loaded["resolved_rows"] == 80
