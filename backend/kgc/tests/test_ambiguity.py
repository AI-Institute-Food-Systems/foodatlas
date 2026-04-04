"""Tests for the ambiguity report — derived from extraction candidates."""

import json
from pathlib import Path

import pandas as pd
from src.pipeline.triplets.ambiguity import (
    AmbiguityRecord,
    AmbiguityReport,
    append_ambiguity_jsonl,
    build_ambiguity_from_extractions,
    write_ambiguity_summary,
)
from src.stores.extraction_store import ExtractionStore
from src.stores.schema import (
    FILE_AMBIGUITY_JSONL,
    FILE_AMBIGUITY_SUMMARY,
    FILE_EXTRACTIONS,
)


def _record(eid: str = "ex_abc") -> AmbiguityRecord:
    return AmbiguityRecord(
        extraction_id=eid,
        extractor="ie",
        head_name_raw="fruit",
        tail_name_raw="vitamin c",
        head_candidates=["e0", "e99"],
        tail_candidates=["e1"],
    )


class TestAmbiguityReport:
    def test_empty_report(self) -> None:
        report = AmbiguityReport()
        assert report.ambiguous_count == 0

    def test_counts(self) -> None:
        report = AmbiguityReport(records=[_record(), _record("ex_def")])
        assert report.ambiguous_count == 2


class TestAppendAmbiguityJsonl:
    def test_appends_records(self, tmp_path: Path) -> None:
        report = AmbiguityReport(records=[_record()])
        append_ambiguity_jsonl(report, tmp_path)

        out = tmp_path / FILE_AMBIGUITY_JSONL
        assert out.exists()
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 1
        rec = json.loads(lines[0])
        assert rec["extraction_id"] == "ex_abc"
        assert rec["head_candidates"] == ["e0", "e99"]

    def test_appends_across_calls(self, tmp_path: Path) -> None:
        append_ambiguity_jsonl(AmbiguityReport(records=[_record("ex1")]), tmp_path)
        append_ambiguity_jsonl(AmbiguityReport(records=[_record("ex2")]), tmp_path)

        lines = (tmp_path / FILE_AMBIGUITY_JSONL).read_text().strip().split("\n")
        assert len(lines) == 2

    def test_empty_report_not_written(self, tmp_path: Path) -> None:
        append_ambiguity_jsonl(AmbiguityReport(), tmp_path)
        assert not (tmp_path / FILE_AMBIGUITY_JSONL).exists()


class TestWriteAmbiguitySummary:
    def test_writes_summary(self, tmp_path: Path) -> None:
        append_ambiguity_jsonl(AmbiguityReport(records=[_record()]), tmp_path)
        write_ambiguity_summary(tmp_path)

        data = json.loads((tmp_path / FILE_AMBIGUITY_SUMMARY).read_text())
        assert data["total_ambiguous"] == 1
        assert data["by_extractor"]["ie"] == 1


class TestBuildAmbiguityFromExtractions:
    def test_finds_ambiguous(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                {
                    "extraction_id": "ex1",
                    "evidence_id": "ev1",
                    "extractor": "ie",
                    "head_name_raw": "fruit",
                    "tail_name_raw": "vitamin c",
                    "head_candidates": ["e0", "e99"],
                    "tail_candidates": ["e1"],
                },
                {
                    "extraction_id": "ex2",
                    "evidence_id": "ev2",
                    "extractor": "fdc",
                    "head_name_raw": "apple",
                    "tail_name_raw": "vitamin c",
                    "head_candidates": ["e0"],
                    "tail_candidates": ["e1"],
                },
            ]
        )
        df.to_parquet(tmp_path / FILE_EXTRACTIONS, index=False)
        store = ExtractionStore(path=tmp_path / FILE_EXTRACTIONS)
        report = build_ambiguity_from_extractions(store)

        assert report.ambiguous_count == 1
        assert report.records[0].extraction_id == "ex1"

    def test_empty_store(self, tmp_path: Path) -> None:
        pd.DataFrame().to_parquet(tmp_path / FILE_EXTRACTIONS)
        store = ExtractionStore(path=tmp_path / FILE_EXTRACTIONS)
        report = build_ambiguity_from_extractions(store)
        assert report.ambiguous_count == 0
