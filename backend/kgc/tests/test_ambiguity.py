"""Tests for the ambiguity report model and writer."""

import json
from pathlib import Path

import pandas as pd
from src.pipeline.triplets.ambiguity import (
    AmbiguityRecord,
    AmbiguityReport,
    collect_ambiguity,
    write_ambiguity_report,
)
from src.stores.schema import FILE_AMBIGUITY_REPORT


def _record(name: str = "aspirin", n_candidates: int = 2) -> AmbiguityRecord:
    ids = [f"e{i}" for i in range(n_candidates)]
    return AmbiguityRecord(
        name_or_id=name,
        entity_type="chemical",
        candidate_ids=ids,
        candidate_names=[f"name_{i}" for i in range(n_candidates)],
        source="chebi",
        triplets_produced=4,
    )


class TestAmbiguityReport:
    def test_empty_report(self) -> None:
        report = AmbiguityReport()
        assert report.ambiguous_count == 0
        assert report.total_triplets_from_ambiguity == 0

    def test_counts(self) -> None:
        records = [_record() for _ in range(2)]
        for r in records:
            r.triplets_produced = 3
        report = AmbiguityReport(records=records)
        assert report.ambiguous_count == 2
        assert report.total_triplets_from_ambiguity == 6


class TestWriteAmbiguityReport:
    def test_writes_json(self, tmp_path: Path) -> None:
        report = AmbiguityReport(records=[_record()])
        write_ambiguity_report(report, tmp_path)

        out = tmp_path / FILE_AMBIGUITY_REPORT
        assert out.exists()
        data = json.loads(out.read_text())
        assert data["ambiguous_count"] == 1
        assert len(data["records"]) == 1
        assert data["records"][0]["name_or_id"] == "aspirin"

    def test_empty_report_not_written(self, tmp_path: Path) -> None:
        report = AmbiguityReport()
        write_ambiguity_report(report, tmp_path)
        assert not (tmp_path / FILE_AMBIGUITY_REPORT).exists()

    def test_caps_samples(self, tmp_path: Path) -> None:
        records = [_record(name=f"chem_{i}") for i in range(250)]
        report = AmbiguityReport(records=records)
        write_ambiguity_report(report, tmp_path)

        data = json.loads((tmp_path / FILE_AMBIGUITY_REPORT).read_text())
        assert data["ambiguous_count"] == 250
        assert data["sample_count"] == 200


class TestCollectAmbiguity:
    def test_filters_ambiguous_only(self) -> None:
        entities = pd.DataFrame(
            [
                {"common_name": "aspirin", "entity_type": "chemical"},
                {"common_name": "ibuprofen", "entity_type": "chemical"},
                {"common_name": "water", "entity_type": "chemical"},
            ],
            index=pd.Index(["e0", "e1", "e2"], name="foodatlas_id"),
        )
        id_map = {
            "CHEBI:100": ["e0", "e1"],  # ambiguous
            "CHEBI:200": ["e2"],  # not ambiguous
        }
        records = collect_ambiguity(id_map, entities, "chemical", "chebi")
        assert len(records) == 1
        assert records[0].name_or_id == "CHEBI:100"
        assert records[0].candidate_ids == ["e0", "e1"]
        assert records[0].candidate_names == ["aspirin", "ibuprofen"]

    def test_empty_map_returns_empty(self) -> None:
        entities = pd.DataFrame()
        assert collect_ambiguity({}, entities, "food", "foodon") == []
