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


def _mark_done(extraction: Path, run: str) -> None:
    (extraction / run).mkdir(parents=True, exist_ok=True)
    (extraction / run / "extraction_predicted.tsv").write_text("pmcid\tresponse\n")


def _touch(bioc: Path, pmcid: str, day: tuple[int, int, int]) -> None:
    bioc.mkdir(parents=True, exist_ok=True)
    path = bioc / f"{pmcid}.xml"
    path.write_text("{}")
    stamp = datetime(*day, 12).timestamp()
    os.utime(path, (stamp, stamp))


class TestRebuild:
    def test_derives_pending_from_run_dirs_and_cache(self, tmp_path: Path) -> None:
        """A finished run that held the file at the time → extracted. A file
        fetched *after* a finished run, or by an unfinished one → still owed.
        Runs past the TTL are ignored; queries merge across sightings."""
        search, extraction, bioc = (
            tmp_path / "search",
            tmp_path / "ext",
            tmp_path / "bioc",
        )
        july, october, ancient = "2026_07_10", "2026_10_16", "2025_01_01"
        _write_results(
            search / july / "query_uid_results.tsv",
            [("1", "PMC1", Q), ("2", "PMC2", "['cocoa']"), ("3", "PMC3", Q)],
        )
        _write_results(
            search / october / "query_uid_results.tsv",
            [("3", "PMC3", Q), ("4", "PMC4", Q), ("2", "PMC2", "['chocolate']")],
        )
        _write_results(search / ancient / "query_uid_results.tsv", [("9", "PMC9", Q)])
        _mark_done(extraction, july)
        _touch(bioc, "PMC1", (2026, 7, 10))
        _touch(bioc, "PMC3", (2026, 10, 16))
        _touch(bioc, "PMC4", (2026, 10, 16))

        ledger = rebuild(search, extraction, bioc, {"PMC1", "PMC3", "PMC4"}, TODAY)

        assert ledger.path == search / LEDGER_NAME
        assert set(ledger.rows) == {"PMC2", "PMC3", "PMC4"}
        assert ledger.rows["PMC2"].first_seen == july
        assert ledger.rows["PMC2"].queries == "['chocolate', 'cocoa']"
        assert ledger.rows["PMC3"].first_seen == july
        assert ledger.rows["PMC4"].first_seen == october
        assert {r.handed_to for r in ledger.rows.values()} == {""}
        assert {r.next_attempt for r in ledger.rows.values()} == {TODAY}
        assert ledger.due(TODAY) == ["PMC2", "PMC3", "PMC4"]

    def test_empty_search_dir_yields_empty_ledger(self, tmp_path: Path) -> None:
        (tmp_path / "search").mkdir()
        ledger = rebuild(
            tmp_path / "search", tmp_path / "ext", tmp_path / "bioc", set(), TODAY
        )
        assert ledger.rows == {}
