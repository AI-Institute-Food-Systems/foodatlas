"""Tests for the pending-article ledger: the schedule that gives an article
more than one fetch attempt, and the hand-over that survives a failed run."""

from __future__ import annotations

import csv
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from src.pipeline.search.pending_ledger import (
    LEDGER_NAME,
    PendingLedger,
    PendingRow,
    append_result_rows,
    read_result_rows,
    summary_line,
)
from src.pipeline.search.runner import run_search

if TYPE_CHECKING:
    from pathlib import Path

TODAY = "2026_10_26"
LAST_WEEK = "2026_10_19"
Q = "['cocoa', 'theobroma cacao']"


def _write_results(path: Path, rows: list[tuple[str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(["pmid", "pmcid", "queries"])
        writer.writerows(rows)


def _pmcids(path: Path) -> list[str]:
    return [pmcid for _, pmcid, _ in read_result_rows(path)]


def _row(
    pmcid: str,
    *,
    first_seen: str = TODAY,
    attempts: int = 0,
    next_attempt: str = TODAY,
    handed_to: str = "",
) -> PendingRow:
    return PendingRow("1", pmcid, Q, first_seen, attempts, next_attempt, handed_to)


def _mark_done(extraction: Path, run: str) -> None:
    (extraction / run).mkdir(parents=True, exist_ok=True)
    (extraction / run / "extraction_predicted.tsv").write_text("pmcid\tresponse\n")


@pytest.fixture
def results(tmp_path: Path) -> Path:
    return tmp_path / "search" / TODAY / "query_uid_results.tsv"


@pytest.fixture
def extraction(tmp_path: Path) -> Path:
    return tmp_path / "extraction"


@pytest.fixture
def ledger(tmp_path: Path) -> PendingLedger:
    return PendingLedger(tmp_path / "search" / LEDGER_NAME)


class TestResultRows:
    def test_read_skips_rows_without_a_wellformed_pmcid(self, results: Path) -> None:
        _write_results(results, [("1", "PMC1", Q), ("2", "", Q), ("3", "unknown", Q)])
        assert _pmcids(results) == ["PMC1"]

    def test_read_missing_file_is_empty(self, results: Path) -> None:
        assert read_result_rows(results) == []

    def test_append_creates_header_and_dedupes(self, results: Path) -> None:
        results.parent.mkdir(parents=True)
        first = append_result_rows(
            results, [("1", "PMC1", Q), ("1", "PMC1", Q), ("2", "PMC2", Q)]
        )
        assert first == 2
        assert _pmcids(results) == ["PMC1", "PMC2"]
        again = append_result_rows(results, [("2", "PMC2", Q), ("3", "PMC3", Q)])
        assert again == 1
        assert _pmcids(results) == ["PMC1", "PMC2", "PMC3"]


class TestLoadSave:
    def test_missing_file_loads_empty(self, tmp_path: Path) -> None:
        assert PendingLedger.load(tmp_path / LEDGER_NAME).rows == {}

    def test_roundtrip_is_lossless_and_atomic(self, ledger: PendingLedger) -> None:
        ledger.rows["PMC2"] = _row("PMC2", attempts=2, next_attempt="2026_12_07")
        ledger.rows["PMC1"] = _row("PMC1", handed_to=LAST_WEEK)
        ledger.save()
        loaded = PendingLedger.load(ledger.path)
        assert loaded.rows == ledger.rows
        assert not ledger.path.with_suffix(".tsv.tmp").exists()


class TestBeginRun:
    def test_this_runs_rows_are_added_and_cached_ones_handed(
        self, ledger: PendingLedger, results: Path, extraction: Path
    ) -> None:
        _write_results(results, [("1", "PMC1", Q), ("2", "PMC2", Q)])
        counts = ledger.begin_run(TODAY, results, {"PMC2"}, extraction)
        assert counts == {
            "expired": 0,
            "done": 0,
            "rehanded": 0,
            "added": 2,
            "recovered": 0,
        }
        assert ledger.rows["PMC1"].handed_to == ""
        assert ledger.rows["PMC2"].handed_to == TODAY
        assert ledger.due(TODAY) == ["PMC1"]
        assert _pmcids(results) == ["PMC1", "PMC2"]

    def test_backlog_row_now_cached_is_handed_and_appended(
        self, ledger: PendingLedger, results: Path, extraction: Path
    ) -> None:
        """Fetched outside a run (or by a run that crashed before saving):
        hand it over now rather than waiting for a fetch that would be a no-op."""
        ledger.rows["PMC9"] = _row("PMC9", first_seen=LAST_WEEK)
        _write_results(results, [("1", "PMC1", Q)])
        counts = ledger.begin_run(TODAY, results, {"PMC9"}, extraction)
        assert counts["recovered"] == 1
        assert _pmcids(results) == ["PMC1", "PMC9"]
        assert ledger.rows["PMC9"].handed_to == TODAY

    def test_resighted_article_merges_queries_and_keeps_first_seen(
        self, ledger: PendingLedger, results: Path, extraction: Path
    ) -> None:
        ledger.rows["PMC1"] = PendingRow(
            "1", "PMC1", "['cocoa']", LAST_WEEK, 0, TODAY, ""
        )
        _write_results(results, [("1", "PMC1", "['chocolate']")])
        counts = ledger.begin_run(TODAY, results, set(), extraction)
        assert counts["added"] == 0
        assert ledger.rows["PMC1"].queries == "['chocolate', 'cocoa']"
        assert ledger.rows["PMC1"].first_seen == LAST_WEEK

    def test_rows_older_than_ttl_expire(
        self, ledger: PendingLedger, results: Path, extraction: Path
    ) -> None:
        ledger.rows["PMC1"] = _row("PMC1", first_seen="2025_10_25")
        ledger.rows["PMC2"] = _row("PMC2", first_seen="2025_10_26")
        counts = ledger.begin_run(TODAY, results, set(), extraction)
        assert counts["expired"] == 1
        assert set(ledger.rows) == {"PMC2"}

    def test_handed_to_finished_run_is_done(
        self, ledger: PendingLedger, results: Path, extraction: Path
    ) -> None:
        ledger.rows["PMC1"] = _row("PMC1", handed_to=LAST_WEEK)
        _mark_done(extraction, LAST_WEEK)
        counts = ledger.begin_run(TODAY, results, set(), extraction)
        assert counts["done"] == 1
        assert ledger.rows == {}
        assert not results.exists()

    def test_handed_to_unfinished_run_is_rehanded_once(
        self, ledger: PendingLedger, results: Path, extraction: Path
    ) -> None:
        """The run that received it never finished extraction: hand it to
        today's run instead — and only once, even if today's run resumes."""
        ledger.rows["PMC1"] = _row("PMC1", handed_to=LAST_WEEK)
        counts = ledger.begin_run(TODAY, results, set(), extraction)
        assert counts["rehanded"] == 1
        assert _pmcids(results) == ["PMC1"]
        assert ledger.rows["PMC1"].handed_to == TODAY
        resumed = ledger.begin_run(TODAY, results, set(), extraction)
        assert resumed["rehanded"] == 0
        assert _pmcids(results) == ["PMC1"]


class TestDueAndFetch:
    def test_not_in_oa_doubles_the_wait(
        self, ledger: PendingLedger, results: Path
    ) -> None:
        ledger.rows["PMC1"] = _row("PMC1")
        first = ledger.record_fetch(TODAY, ["PMC1"], {"PMC1"}, set(), results)
        assert first == {"recovered": 0, "deferred": 1}
        row = ledger.rows["PMC1"]
        assert (row.attempts, row.next_attempt) == (1, "2026_11_09")
        assert ledger.due(TODAY) == []
        assert ledger.due("2026_11_09") == ["PMC1"]
        ledger.record_fetch("2026_11_09", ["PMC1"], {"PMC1"}, set(), results)
        assert (row.attempts, row.next_attempt) == (2, "2026_12_07")

    def test_transient_error_leaves_it_due_next_run(
        self, ledger: PendingLedger, results: Path
    ) -> None:
        ledger.rows["PMC1"] = _row("PMC1")
        ledger.record_fetch(TODAY, ["PMC1"], set(), {"PMC1"}, results)
        row = ledger.rows["PMC1"]
        assert (row.attempts, row.next_attempt, row.handed_to) == (0, TODAY, "")
        assert ledger.due(TODAY) == ["PMC1"]

    def test_obtained_backlog_row_is_appended_and_handed(
        self, ledger: PendingLedger, results: Path
    ) -> None:
        ledger.rows["PMC1"] = _row("PMC1")
        ledger.rows["PMC9"] = _row("PMC9", first_seen=LAST_WEEK)
        _write_results(results, [("1", "PMC1", Q)])
        outcome = ledger.record_fetch(TODAY, ["PMC1", "PMC9"], set(), set(), results)
        assert outcome == {"recovered": 1, "deferred": 0}
        assert _pmcids(results) == ["PMC1", "PMC9"]
        assert {r.handed_to for r in ledger.rows.values()} == {TODAY}

    def test_pmcid_not_in_ledger_is_ignored(
        self, ledger: PendingLedger, results: Path
    ) -> None:
        outcome = ledger.record_fetch(TODAY, ["PMC404"], set(), set(), results)
        assert outcome == {"recovered": 0, "deferred": 0}


class TestSummary:
    def test_line_names_every_count(self) -> None:
        line = summary_line({"added": 3, "retried": 2, "recovered": 1})
        assert line == (
            "pending ledger: 3 added, 2 retried from backlog, 1 recovered, "
            "0 deferred, 0 re-handed, 0 done, 0 expired, 0 carried"
        )


class TestRunSearchIntegration:
    def test_backlog_rides_along_with_this_weeks_fetch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outputs = tmp_path / "outputs"
        results = outputs / "search" / TODAY / "query_uid_results.tsv"
        _write_results(results, [("1", "PMC1", Q)])
        ledger = PendingLedger(outputs / "search" / LEDGER_NAME)
        ledger.rows["PMC9"] = _row("PMC9", first_seen=LAST_WEEK)
        ledger.save()

        monkeypatch.setattr(
            "src.pipeline.search.runner.get_pmcid_pmid_mapping", lambda: ({}, {})
        )
        monkeypatch.setattr(
            "src.pipeline.search.runner.load_data", lambda *a, **k: ({}, set())
        )
        monkeypatch.setattr(
            "src.pipeline.search.runner.parse_query", lambda q: ["cocoa"]
        )
        monkeypatch.setattr("src.pipeline.search.runner.search_queries", MagicMock())
        monkeypatch.setattr(
            "src.pipeline.search.runner._scan_cached_pmcids", lambda p: set()
        )
        fetch = MagicMock(
            return_value=MagicMock(fetched=2, cached=0, not_in_oa=[], errors=[])
        )
        monkeypatch.setattr("src.pipeline.search.runner.fetch_missing", fetch)

        run_search(
            query="cocoa",
            query_uid_results_filepath=str(results),
            filepath_bioc_pmc=str(tmp_path / "bioc"),
            output_base_dir=str(outputs / "search"),
            current_date=TODAY,
            extraction_base_dir=str(outputs / "extraction"),
        )

        assert fetch.call_args[0][0] == ["PMC1", "PMC9"]
        assert _pmcids(results) == ["PMC1", "PMC9"]
        saved = PendingLedger.load(outputs / "search" / LEDGER_NAME)
        assert {p: r.handed_to for p, r in saved.rows.items()} == {
            "PMC1": TODAY,
            "PMC9": TODAY,
        }
