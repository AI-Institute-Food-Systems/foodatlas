"""Tests for the ambiguity report — derived from extraction candidates."""

import json
from pathlib import Path

import pandas as pd
from src.pipeline.triplets.ambiguity import (
    AmbiguityRecord,
    AmbiguityReport,
    build_ambiguity_from_extractions,
    write_ambiguity_report,
)
from src.stores.extraction_store import ExtractionStore
from src.stores.schema import FILE_AMBIGUITY_REPORT, FILE_EXTRACTIONS


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


class TestWriteAmbiguityReport:
    def test_writes_json(self, tmp_path: Path) -> None:
        report = AmbiguityReport(records=[_record()])
        write_ambiguity_report(report, tmp_path)

        out = tmp_path / FILE_AMBIGUITY_REPORT
        assert out.exists()
        data = json.loads(out.read_text())
        assert data["ambiguous_count"] == 1
        assert data["records"][0]["extraction_id"] == "ex_abc"
        assert data["records"][0]["head_candidates"] == ["e0", "e99"]

    def test_empty_report_not_written(self, tmp_path: Path) -> None:
        report = AmbiguityReport()
        write_ambiguity_report(report, tmp_path)
        assert not (tmp_path / FILE_AMBIGUITY_REPORT).exists()


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
