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
    bioc_dir: Path,
    cached: set[str],
    today: str,
) -> PendingLedger:
    """Rebuild the ledger from run dirs no older than ``TTL_DAYS``.

    An article counts as extracted if a finished run listed it while its BioC
    file was already on disk (file mtime no later than the day after the run).
    Everything else a run wanted is pending and due now; the next weekly run
    hands the cached ones straight to retrieval.
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
        for pmid, pmcid, queries in read_result_rows(run_dir / "query_uid_results.tsv"):
            if pmcid in extracted:
                continue
            if finished and pmcid in cached and _fetched_by(bioc_dir, pmcid, run):
                extracted.add(pmcid)
                ledger.rows.pop(pmcid, None)
                continue
            row = ledger.rows.get(pmcid)
            if row is None:
                ledger.rows[pmcid] = PendingRow(pmid, pmcid, queries, run, 0, today, "")
            else:
                row.queries = _merge_queries(row.queries, queries)
    return ledger


def _fetched_by(bioc_dir: Path, pmcid: str, run: str) -> bool:
    """True if the BioC file was on disk by the day after *run*."""
    mtime = (bioc_dir / f"{pmcid}.xml").stat().st_mtime
    return _fmt(date.fromtimestamp(mtime)) <= _days_after(run, 1)
