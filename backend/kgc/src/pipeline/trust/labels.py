"""Score → label mapping for trust signals.

Labels are *derived*, never stored. The DB only holds the float score; query
sites map to a label via this helper. Thresholds are caller-tunable so the
API layer (or a SQL view) can experiment without rewriting data.
"""

from __future__ import annotations

from typing import Literal

TrustLabel = Literal["implausible", "suspicious", "plausible"]


def label_from_score(
    score: float,
    *,
    suspicious_at: float = 0.33,
    plausible_at: float = 0.67,
) -> TrustLabel:
    """Map a 0..1 score to one of three labels.

    Boundaries are inclusive on the lower bound: ``score == suspicious_at``
    counts as ``"suspicious"`` (not ``"implausible"``); ``score == plausible_at``
    counts as ``"plausible"``.
    """
    if score < suspicious_at:
        return "implausible"
    if score < plausible_at:
        return "suspicious"
    return "plausible"
