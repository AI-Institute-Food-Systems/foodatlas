"""Tests for rebuilding the pending ledger from run directories: what counts
as already extracted, what is still owed, and which runs are too old to matter."""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import TYPE_CHECKING

from src.pipeline.search.pending_ledger import LEDGER_NAME
from src.pipeline.search.pending_rebuild import rebuild

if TYPE_CHECKING:
    from pathlib import Path

TODAY = "2026_10_26"
Q = "['cocoa', 'theobroma cacao']"


def _write_results(path: Path, rows: list[tuple[str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(["pmid", "pmcid", "queries"])
        writer.writerows(rows)


def _stamp(path: Path, year: int, month: int, day: int, hour: int = 12) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("{}")
    moment = datetime(year, month, day, hour).timestamp()
    os.utime(path, (moment, moment))


class _Runs:
    """One tmp_path laid out like outputs/: search/, extraction/, retrieval/, bioc/."""

    def __init__(self, root: Path) -> None:
        self.search = root / "search"
        self.extraction = root / "extraction"
        self.retrieval = root / "retrieval"
        self.bioc = root / "bioc"

    def results(self, run: str, rows: list[tuple[str, str, str]]) -> None:
        _write_results(self.search / run / "query_uid_results.tsv", rows)

    def finished(
        self, run: str, retrieval_at: tuple[int, int, int, int] | None
    ) -> None:
        _stamp(self.extraction / run / "extraction_predicted.tsv", 2026, 1, 1)
        if retrieval_at:
            _stamp(self.retrieval / run / "sentence_filtering_input.tsv", *retrieval_at)

    def fetched(self, pmcid: str, at: tuple[int, int, int, int]) -> None:
        _stamp(self.bioc / f"{pmcid}.xml", *at)

    def rebuild(self, cached: set[str]) -> dict[str, tuple[str, str]]:
        ledger = rebuild(
            self.search, self.extraction, self.retrieval, self.bioc, cached, TODAY
        )
        assert ledger.path == self.search / LEDGER_NAME
        assert {r.handed_to for r in ledger.rows.values()} <= {""}
        assert {r.next_attempt for r in ledger.rows.values()} <= {TODAY}
        return {p: (r.first_seen, r.queries) for p, r in ledger.rows.items()}


class TestRebuild:
    def test_finished_run_extracts_what_was_on_disk_when_it_read_the_cache(
        self, tmp_path: Path
    ) -> None:
        """PMC1 was fetched before July's retrieval ran → extracted. PMC3's
        file arrived in October → July never read it → still owed. PMC2 is
        not cached → owed, with queries merged across both sightings."""
        runs = _Runs(tmp_path)
        july, october, ancient = "2026_07_10", "2026_10_16", "2025_01_01"
        runs.results(
            july, [("1", "PMC1", Q), ("2", "PMC2", "['cocoa']"), ("3", "PMC3", Q)]
        )
        runs.results(
            october,
            [("3", "PMC3", Q), ("4", "PMC4", Q), ("2", "PMC2", "['chocolate']")],
        )
        runs.results(ancient, [("9", "PMC9", Q)])
        runs.finished(july, retrieval_at=(2026, 7, 10, 7))
        runs.fetched("PMC1", (2026, 7, 10, 6))
        runs.fetched("PMC3", (2026, 10, 16, 6))
        runs.fetched("PMC4", (2026, 10, 16, 6))

        pending = runs.rebuild({"PMC1", "PMC3", "PMC4"})

        assert pending == {
            "PMC2": (july, "['chocolate', 'cocoa']"),
            "PMC3": (july, Q),
            "PMC4": (october, Q),
        }

    def test_run_rerun_by_hand_days_later_is_judged_by_when_retrieval_ran(
        self, tmp_path: Path
    ) -> None:
        """The 2026_10_02 run was executed on Oct 6. A file fetched on Oct 6
        before its retrieval was read by it; the run-date heuristic would have
        re-queued it. A file fetched after that retrieval is genuinely owed."""
        runs = _Runs(tmp_path)
        run = "2026_10_02"
        runs.results(run, [("1", "PMC1", Q), ("2", "PMC2", Q)])
        runs.finished(run, retrieval_at=(2026, 10, 6, 22))
        runs.fetched("PMC1", (2026, 10, 6, 21))
        runs.fetched("PMC2", (2026, 10, 7, 9))

        assert set(runs.rebuild({"PMC1", "PMC2"})) == {"PMC2"}

    def test_without_retrieval_output_falls_back_to_the_day_after_the_run(
        self, tmp_path: Path
    ) -> None:
        runs = _Runs(tmp_path)
        run = "2026_07_10"
        runs.results(run, [("1", "PMC1", Q), ("2", "PMC2", Q)])
        runs.finished(run, retrieval_at=None)
        runs.fetched("PMC1", (2026, 7, 11, 23))
        runs.fetched("PMC2", (2026, 7, 12, 1))

        assert set(runs.rebuild({"PMC1", "PMC2"})) == {"PMC2"}

    def test_empty_search_dir_yields_empty_ledger(self, tmp_path: Path) -> None:
        runs = _Runs(tmp_path)
        runs.search.mkdir()
        assert runs.rebuild(set()) == {}
