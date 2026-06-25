"""Pure precision / recall / F1 arithmetic over judge verdicts.

Two match levels:
  L1 (relation): a (food, chemical) pair is correct if it is stated by the sentence.
  L2 (strict):   additionally requires the concentration to agree (order of magnitude).
                 A relation-correct but concentration-wrong extraction is counted as
                 both a false positive (model asserted the wrong amount) and a false
                 negative (the true fact with its amount was missed).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .schema import Adjudication


@dataclass(frozen=True)
class Counts:
    tp: int
    fp: int
    fn: int

    def __add__(self, other: Counts) -> Counts:
        return Counts(self.tp + other.tp, self.fp + other.fp, self.fn + other.fn)


@dataclass(frozen=True)
class PRF:
    precision: float
    recall: float
    f1: float
    tp: int
    fp: int
    fn: int


def prf(counts: Counts) -> PRF:
    """Precision/recall/F1 from raw counts; 0.0 where the denominator is empty."""
    p = counts.tp / (counts.tp + counts.fp) if counts.tp + counts.fp else 0.0
    r = counts.tp / (counts.tp + counts.fn) if counts.tp + counts.fn else 0.0
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    return PRF(p, r, f1, counts.tp, counts.fp, counts.fn)


def counts_from_adjudication(adj: Adjudication) -> tuple[Counts, Counts]:
    """Return (L1, L2) counts for one sentence's adjudication."""
    n_match = len(adj.matches)
    n_conc_ok = sum(1 for m in adj.matches if m.conc_agree)
    n_conc_bad = n_match - n_conc_ok
    fp = len(adj.model_only)
    fn = len(adj.gold_only)

    l1 = Counts(tp=n_match, fp=fp, fn=fn)
    l2 = Counts(tp=n_conc_ok, fp=fp + n_conc_bad, fn=fn + n_conc_bad)
    return l1, l2


def summarize(counts: Counts) -> dict[str, float | int]:
    """Flatten a PRF result to a JSON-friendly dict."""
    m = prf(counts)
    return {
        "precision": round(m.precision, 4),
        "recall": round(m.recall, 4),
        "f1": round(m.f1, 4),
        "tp": m.tp,
        "fp": m.fp,
        "fn": m.fn,
    }
