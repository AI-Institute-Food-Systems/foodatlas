"""Derive the pending ledger from past run directories and the BioC cache.

The ledger is a cache, not a source of truth. Every weekly run leaves behind
``query_uid_results.tsv`` (what it wanted) and, once it finished,
``extraction_predicted.tsv``; together with the BioC cache those reconstruct
which articles are still owed. This is also the one-time backfill for the
weeks before the ledger existed: ``main.py pending --rebuild``.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from .pending_ledger import (
    _RUN_DIR,
    LEDGER_NAME,
    TTL_DAYS,
    PendingLedger,
    PendingRow,
    _days_after,
    _extraction_done,
    _fmt,
    _merge_queries,
    read_result_rows,
)

if TYPE_CHECKING:
    from pathlib import Path


def rebuild(
    search_base: Path,
    extraction_base: Path,
    retrieval_base: Path,
    bioc_dir: Path,
    cached: set[str],
    today: str,
) -> PendingLedger:
    """Rebuild the ledger from run dirs no older than ``TTL_DAYS``.

    An article counts as extracted if a finished run listed it and its BioC
    file was already on disk when that run's retrieval read the cache. Runs are
    sometimes re-run by hand days after their directory's date, so the
    retrieval output's mtime is the reference, not the date; the day after the
    run is the fallback when that output is gone. Everything else a run wanted
    is pending and due now; the next weekly run hands the cached ones straight
    to retrieval.
    """
    cutoff = _days_after(today, -TTL_DAYS)
    ledger = PendingLedger(search_base / LEDGER_NAME)
    extracted: set[str] = set()
    run_dirs = sorted(
        d
        for d in search_base.iterdir()
        if d.is_dir() and _RUN_DIR.match(d.name) and d.name >= cutoff
    )
    for run_dir in run_dirs:
        run = run_dir.name
        finished = _extraction_done(extraction_base, run)
        read_at = _retrieval_time(retrieval_base, run)
        for pmid, pmcid, queries in read_result_rows(run_dir / "query_uid_results.tsv"):
            if pmcid in extracted:
                continue
            if (
                finished
                and pmcid in cached
                and _on_disk_by(bioc_dir, pmcid, read_at, run)
            ):
                extracted.add(pmcid)
                ledger.rows.pop(pmcid, None)
                continue
            row = ledger.rows.get(pmcid)
            if row is None:
                ledger.rows[pmcid] = PendingRow(pmid, pmcid, queries, run, 0, today, "")
            else:
                row.queries = _merge_queries(row.queries, queries)
    return ledger


def _retrieval_time(retrieval_base: Path, run: str) -> float | None:
    """When *run*'s retrieval stage read the cache, or None if its output is gone."""
    path = retrieval_base / run / "sentence_filtering_input.tsv"
    return path.stat().st_mtime if path.is_file() else None


def _on_disk_by(bioc_dir: Path, pmcid: str, read_at: float | None, run: str) -> bool:
    """True if the BioC file existed when *run*'s retrieval read the cache,
    falling back to the day after the run's date when that time is unknown."""
    mtime = (bioc_dir / f"{pmcid}.xml").stat().st_mtime
    if read_at is not None:
        return mtime <= read_at
    return _fmt(date.fromtimestamp(mtime)) <= _days_after(run, 1)
