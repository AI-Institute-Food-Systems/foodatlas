"""Tests for report.format — report formatting."""

from __future__ import annotations

from src.pipeline.report.format import format_report
from src.pipeline.report.runner import (
    EntityDetailChanges,
    EntitySummary,
    KGDiffResult,
    SourceCoverage,
    TripletSummary,
)


def _make_result() -> KGDiffResult:
    return KGDiffResult(
        entity_summary=EntitySummary(
            old_counts={"food": 10, "chemical": 20},
            new_counts={"food": 12, "chemical": 25, "disease": 5},
            new_ids=["e100", "e101"],
            retired_ids=["e50"],
            stable_count=28,
            old_orphans_by_type={"chemical": 3},
            new_orphans_by_type={"chemical": 2, "disease": 1},
        ),
        triplet_summary=TripletSummary(
            old_counts={"r1": 100, "r2": 50},
            new_counts={"r1": 150, "r2": 60, "r3": 10},
            new_count=70,
            removed_count=10,
            stable_count=130,
        ),
        entity_details=EntityDetailChanges(
            name_changes=[("e1", "apple", "Apple")],
            type_changes=[("e2", "flavor", "chemical")],
        ),
        source_coverage=SourceCoverage(
            old_contains_by_source={"fdc": 100},
            old_diseases_by_source={"ctd": 200},
            new_attestations_by_source={"fdc": 120, "chebi": 80},
            new_evidence_by_type={"pubmed": 50},
        ),
    )


class TestFormatReport:
    def test_contains_all_sections(self) -> None:
        report = format_report(_make_result())
        assert "KG DIFF" in report
        assert "Entity Summary" in report
        assert "Triplet Summary" in report
        assert "Entity Detail" in report
        assert "Source Coverage" in report

    def test_entity_counts(self) -> None:
        report = format_report(_make_result())
        assert "food" in report
        assert "chemical" in report
        assert "disease" in report

    def test_triplet_labels(self) -> None:
        report = format_report(_make_result())
        assert "CONTAINS" in report
        assert "IS_A" in report

    def test_name_change_sample(self) -> None:
        report = format_report(_make_result())
        assert "'apple'" in report
        assert "'Apple'" in report

    def test_type_change_sample(self) -> None:
        report = format_report(_make_result())
        assert "flavor -> chemical" in report

    def test_source_names(self) -> None:
        report = format_report(_make_result())
        assert "fdc" in report
        assert "ctd" in report
        assert "chebi" in report

    def test_orphan_section(self) -> None:
        report = format_report(_make_result())
        assert "Orphan entities" in report
        # Old total 3, new total 3 (2 + 1)
        lines = [line for line in report.splitlines() if "TOTAL" in line]
        assert any("3" in line for line in lines)
