"""Ledger of articles PubMed matched that we have not extracted yet.

A weekly search gives a newly published paper one fetch attempt, in the week
PMC is least likely to hold its full text yet, and never asks again. A run
that fails after fetching loses its articles the same way. One rule closes
both gaps: an article leaves the ledger only when a run that received it
finished extraction, or when it is older than ``TTL_DAYS``.

Rows are keyed by PMCID and carry the ``query_uid_results.tsv`` columns, so a
recovered article is handed to a later run together with its own queries.

* ``handed_to == ""`` — not in the BioC cache. Re-fetched on a doubling
  schedule (0, 2, 6, 14, 30 weeks after first sight). Only a genuine
  ``not_in_oa`` advances the schedule; a transient error leaves the row due
  again next run.
* ``handed_to == <run>`` — in the cache and present in that run's results
  file. Dropped once that run has an ``extraction_predicted.tsv``; appended to
  the next run's results file if it never got one.

The file is a cache, not a source of truth: ``pending_rebuild.rebuild`` derives
it from the run directories and the BioC cache.
"""

from __future__ import annotations

import csv
import re
from ast import literal_eval
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import date
    from pathlib import Path

LEDGER_NAME = "pending.tsv"
TTL_DAYS = 365
RESULT_COLUMNS = ("pmid", "pmcid", "queries")
_DATE_FMT = "%Y_%m_%d"
_RUN_DIR = re.compile(r"^\d{4}_\d{2}_\d{2}$")
_PMCID = re.compile(r"^PMC\d+$")

ResultRow = tuple[str, str, str]


def _parse(day: str) -> date:
    return datetime.strptime(day, _DATE_FMT).date()


def _fmt(day: date) -> str:
    return day.strftime(_DATE_FMT)


def _days_after(day: str, days: int) -> str:
    return _fmt(_parse(day) + timedelta(days=days))


def _query_list(queries: str) -> list[str]:
    try:
        parsed = literal_eval(queries) if queries else []
    except (ValueError, SyntaxError):
        return []
    return [str(q) for q in parsed] if isinstance(parsed, list) else []


def _merge_queries(current: str, incoming: str) -> str:
    return repr(sorted(set(_query_list(current)) | set(_query_list(incoming))))


@dataclass
class PendingRow:
    pmid: str
    pmcid: str
    queries: str
    first_seen: str
    attempts: int
    next_attempt: str
    handed_to: str

    def as_result(self) -> ResultRow:
        return (self.pmid, self.pmcid, self.queries)


def read_result_rows(path: Path) -> list[ResultRow]:
    """(pmid, pmcid, queries) rows with a well-formed PMCID; [] if absent."""
    if not path.is_file():
        return []
    with path.open(newline="") as f:
        return [
            (r["pmid"], r["pmcid"], r["queries"])
            for r in csv.DictReader(f, delimiter="\t")
            if _PMCID.match(r["pmcid"])
        ]


def append_result_rows(path: Path, rows: Iterable[ResultRow]) -> int:
    """Append rows whose PMCID the file lacks; returns how many were written."""
    present = {pmcid for _, pmcid, _ in read_result_rows(path)}
    new: list[ResultRow] = []
    for row in rows:
        if row[1] not in present:
            new.append(row)
            present.add(row[1])
    if not new:
        return 0
    header = not path.is_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        if header:
            writer.writerow(RESULT_COLUMNS)
        writer.writerows(new)
    return len(new)


def _extraction_done(extraction_base: Path, run: str) -> bool:
    return (extraction_base / run / "extraction_predicted.tsv").is_file()


class PendingLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.rows: dict[str, PendingRow] = {}

    @classmethod
    def load(cls, path: Path) -> PendingLedger:
        ledger = cls(path)
        if not path.is_file():
            return ledger
        with path.open(newline="") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                ledger.rows[r["pmcid"]] = PendingRow(
                    r["pmid"],
                    r["pmcid"],
                    r["queries"],
                    r["first_seen"],
                    int(r["attempts"]),
                    r["next_attempt"],
                    r["handed_to"],
                )
        return ledger

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        names = [f.name for f in fields(PendingRow)]
        tmp = self.path.with_suffix(".tsv.tmp")
        with tmp.open("w", newline="") as f:
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            writer.writerow(names)
            for row in sorted(self.rows.values(), key=lambda r: r.pmcid):
                writer.writerow([getattr(row, n) for n in names])
        tmp.replace(self.path)

    # -- one weekly run ----------------------------------------------------

    def begin_run(
        self,
        today: str,
        results_path: Path,
        cached: set[str],
        extraction_base: Path,
    ) -> dict[str, int]:
        """Reconcile the ledger with this run before fetching. Returns counts."""
        expired = self._expire(today)
        done, rehanded = self._settle_handed(today, extraction_base, results_path)
        added = self._absorb(today, read_result_rows(results_path))
        recovered = self._hand_cached(today, cached, results_path)
        return {
            "expired": expired,
            "done": done,
            "rehanded": rehanded,
            "added": added,
            "recovered": recovered,
        }

    def due(self, today: str) -> list[str]:
        """PMCIDs not in the cache whose retry date has arrived."""
        return sorted(
            p
            for p, r in self.rows.items()
            if not r.handed_to and r.next_attempt <= today
        )

    def record_fetch(
        self,
        today: str,
        attempted: Iterable[str],
        not_in_oa: set[str],
        errored: set[str],
        results_path: Path,
    ) -> dict[str, int]:
        """Apply one fetch's outcomes. Articles obtained from the backlog are
        appended to *results_path* so retrieval sees them this run."""
        obtained: list[PendingRow] = []
        deferred = 0
        for pmcid in attempted:
            row = self.rows.get(pmcid)
            if row is None:
                continue
            if pmcid in not_in_oa:
                row.attempts += 1
                row.next_attempt = _days_after(today, 7 * 2**row.attempts)
                deferred += 1
            elif pmcid not in errored:
                obtained.append(row)
        recovered = append_result_rows(results_path, (r.as_result() for r in obtained))
        for row in obtained:
            row.handed_to = today
        return {"recovered": recovered, "deferred": deferred}

    # -- steps of begin_run --------------------------------------------------

    def _expire(self, today: str) -> int:
        cutoff = _days_after(today, -TTL_DAYS)
        stale = [p for p, r in self.rows.items() if r.first_seen < cutoff]
        for p in stale:
            del self.rows[p]
        return len(stale)

    def _settle_handed(
        self, today: str, extraction_base: Path, results_path: Path
    ) -> tuple[int, int]:
        """Rows handed to a finished run are done; rows handed to a run that
        never finished are handed again, to *today*."""
        done = [
            p
            for p, r in self.rows.items()
            if r.handed_to
            and r.handed_to != today
            and _extraction_done(extraction_base, r.handed_to)
        ]
        for p in done:
            del self.rows[p]
        rehand = [r for r in self.rows.values() if r.handed_to and r.handed_to != today]
        append_result_rows(results_path, (r.as_result() for r in rehand))
        for row in rehand:
            row.handed_to = today
        return len(done), len(rehand)

    def _absorb(self, today: str, rows: Iterable[ResultRow]) -> int:
        """Register this run's own matches; a re-sighted article merges queries."""
        added = 0
        for pmid, pmcid, queries in rows:
            row = self.rows.get(pmcid)
            if row is None:
                self.rows[pmcid] = PendingRow(pmid, pmcid, queries, today, 0, today, "")
                added += 1
            else:
                row.queries = _merge_queries(row.queries, queries)
        return added

    def _hand_cached(self, today: str, cached: set[str], results_path: Path) -> int:
        """Hand every unhanded row the cache already holds to *today*; returns
        how many came from the backlog (this run's own rows are already there)."""
        ready = [r for r in self.rows.values() if not r.handed_to and r.pmcid in cached]
        recovered = append_result_rows(results_path, (r.as_result() for r in ready))
        for row in ready:
            row.handed_to = today
        return recovered


def summary_line(counts: dict[str, int]) -> str:
    c = {
        k: counts.get(k, 0)
        for k in (
            "added",
            "retried",
            "recovered",
            "deferred",
            "rehanded",
            "done",
            "expired",
            "carried",
        )
    }
    return (
        f"pending ledger: {c['added']} added, {c['retried']} retried from backlog, "
        f"{c['recovered']} recovered, {c['deferred']} deferred, "
        f"{c['rehanded']} re-handed, {c['done']} done, {c['expired']} expired, "
        f"{c['carried']} carried"
    )
