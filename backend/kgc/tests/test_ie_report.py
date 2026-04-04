"""Tests for ie_report — unresolved names diagnostics."""

import json
from pathlib import Path

import pandas as pd
from src.pipeline.ie.report import write_unresolved_report
from src.stores.schema import FILE_IE_UNRESOLVED


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

    def test_writes_jsonl(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        out = write_unresolved_report({"banana"}, {"zinc"}, meta, tmp_path)
        assert out.exists()
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2
        records = [json.loads(line) for line in lines]
        types = {r["entity_type"] for r in records}
        assert types == {"food", "chemical"}

    def test_occurrences_counted(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        out = write_unresolved_report({"banana"}, {"zinc"}, meta, tmp_path)
        lines = out.read_text().strip().split("\n")
        records = [json.loads(line) for line in lines]
        banana = next(r for r in records if r["name"] == "banana")
        assert banana["occurrences"] == 2

    def test_empty_unresolved(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        out = write_unresolved_report(set(), set(), meta, tmp_path)
        content = out.read_text() if out.exists() else ""
        assert content == ""

    def test_appends_across_calls(self, tmp_path: Path) -> None:
        meta = self._make_metadata()
        write_unresolved_report({"banana"}, set(), meta, tmp_path)
        write_unresolved_report(set(), {"zinc"}, meta, tmp_path)
        out = tmp_path / FILE_IE_UNRESOLVED
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2
