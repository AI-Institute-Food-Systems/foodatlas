"""Per-attestation trust-score filtering applied to composition responses.

Each evidence dict in the materialised composition view (``fdc_evidences``,
``foodatlas_evidences``, ``dmd_evidences``) carries a list of ``extraction``
objects, each annotated with an ``attestation_id``. Trust signals live in
``base_trust_signals`` and are joined at request time by attestation_id. The
threshold is configurable via :class:`APISettings.trust_low_threshold`.

Modes:

- ``default`` — drop extractions whose latest llm_plausibility score is below
  the threshold; if a row's evidences end up entirely empty, drop the row.
- ``show_all`` — return every extraction; annotate each with ``trust_low``
  (``True`` when the extraction has a score and that score is below the
  threshold; ``False`` otherwise — including for unscored attestations like
  FDC/DMD which we treat as trusted). The frontend uses ``trust_low`` to
  render a warning badge on rows with at least one low-trust extraction.
- ``low_only`` — keep only extractions whose score is below the threshold;
  drop rows that end up with no evidences. Used when the user clicks the
  warning badge to filter to suspicious data points (mirrors the
  "ambiguous" pattern).

Attestations without a trust signal default to ``score = 1.0`` (trusted).
This means FDC and DMD-derived attestations — which the trust stage doesn't
judge today — pass through ``default`` and are correctly excluded from
``low_only``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

TrustMode = Literal["default", "show_all", "low_only"]
_EVIDENCE_KEYS = ("fdc_evidences", "foodatlas_evidences", "dmd_evidences")
_NO_SIGNAL_DEFAULT_SCORE = 1.0


async def apply_trust_filter(
    session: AsyncSession,
    rows: list[dict],
    *,
    mode: TrustMode,
    threshold: float,
) -> list[dict]:
    """Filter or annotate ``rows`` according to ``mode``; returns new list.

    ``rows`` is a list of MV-shaped dicts (each may contain
    ``fdc_evidences`` / ``foodatlas_evidences`` / ``dmd_evidences`` keys
    with list-or-None values). Every extraction must already carry
    ``attestation_id`` (added in the materializer).
    """
    if not rows:
        return rows

    att_ids = _collect_attestation_ids(rows)

    if mode == "show_all":
        if not att_ids:
            return rows
        scores = await _fetch_trust_scores(session, att_ids)
        return _annotate_rows(rows, scores=scores, threshold=threshold)

    if not att_ids:
        # Nothing to score; default mode keeps everything (treats as
        # high-trust); low_only mode drops everything since no row has
        # any low-trust evidence.
        return rows if mode == "default" else []

    scores = await _fetch_trust_scores(session, att_ids)
    return _filter_rows(rows, scores=scores, mode=mode, threshold=threshold)


def _annotate_rows(
    rows: list[dict],
    *,
    scores: dict[str, float],
    threshold: float,
) -> list[dict]:
    """Add ``trust_low`` to every extraction without dropping anything."""
    out: list[dict] = []
    for row in rows:
        new_row = dict(row)
        for key in _EVIDENCE_KEYS:
            evs = row.get(key)
            if not evs:
                continue
            new_row[key] = [
                {
                    **ev,
                    "extraction": [
                        {**ext, "trust_low": _is_low(ext, scores, threshold)}
                        for ext in (ev.get("extraction") or [])
                    ],
                }
                for ev in evs
            ]
        out.append(new_row)
    return out


def _is_low(ext: dict, scores: dict[str, float], threshold: float) -> bool:
    """``True`` only when the extraction has a score AND it's at-or-below threshold.

    Threshold is inclusive on the low side (``<=``): a score of exactly the
    threshold counts as low-trust. The model often anchors on round numbers
    (e.g. judges "suspicious-but-not-impossible" cases as exactly 0.3); using
    ``<=`` catches those instead of letting them slip through.

    Unscored attestations (FDC / DMD / un-judged lit2kg) are *not* flagged —
    we don't know their plausibility and shouldn't visually penalize them.
    """
    aid = ext.get("attestation_id", "")
    if aid not in scores:
        return False
    return scores[aid] <= threshold


def _collect_attestation_ids(rows: list[dict]) -> list[str]:
    seen: set[str] = set()
    for row in rows:
        for key in _EVIDENCE_KEYS:
            for ev in row.get(key) or []:
                for ext in ev.get("extraction") or []:
                    aid = ext.get("attestation_id")
                    if aid:
                        seen.add(aid)
    return list(seen)


async def _fetch_trust_scores(
    session: AsyncSession, att_ids: list[str]
) -> dict[str, float]:
    """Latest valid llm_plausibility score per attestation; -1 errors excluded.

    "Latest" = highest ``created_at`` for the attestation. Re-judging an
    attestation with a new prompt (different ``config_hash``) appends a new
    row to ``base_trust_signals`` rather than overwriting the old one, so
    old judgments are preserved for audit. The API picks the most recent
    one — without this, a prompt change wouldn't take visual effect for
    attestations whose old score was higher than the new score.
    """
    result = await session.execute(
        text("""
            WITH ranked AS (
                SELECT
                    attestation_id,
                    score,
                    ROW_NUMBER() OVER (
                        PARTITION BY attestation_id
                        ORDER BY created_at DESC
                    ) AS rn
                FROM base_trust_signals
                WHERE attestation_id = ANY(:ids)
                  AND signal_kind = 'llm_plausibility'
                  AND score >= 0
            )
            SELECT attestation_id, score FROM ranked WHERE rn = 1
        """),
        {"ids": att_ids},
    )
    return {row.attestation_id: float(row.score) for row in result}


def _filter_rows(
    rows: list[dict],
    *,
    scores: dict[str, float],
    mode: TrustMode,
    threshold: float,
) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        new_row = dict(row)
        any_kept = False
        for key in _EVIDENCE_KEYS:
            new_evs = _filter_evidences(
                row.get(key) or [], scores=scores, mode=mode, threshold=threshold
            )
            new_row[key] = new_evs or None
            if new_evs:
                any_kept = True
        if not any_kept:
            continue
        # The MV's median_concentration is computed at materialize time across
        # every extraction; once we drop low-trust ones it can become stale
        # (e.g. "tomato linoleic acid 52410 mg/100g" still showing as the
        # median after the offending rows are filtered). Recompute from what
        # actually survived the filter so the UI's median matches its rows.
        new_row["median_concentration"] = _recompute_median(new_row)
        out.append(new_row)
    return out


def _recompute_median(row: dict) -> dict | None:
    """Median of mg/100g values across surviving extractions; None if empty."""
    vals: list[float] = []
    for key in _EVIDENCE_KEYS:
        for ev in row.get(key) or []:
            for ext in ev.get("extraction") or []:
                conc = ext.get("converted_concentration") or {}
                val = conc.get("value")
                unit = conc.get("unit") or ""
                if val is None or unit != "mg/100g":
                    continue
                try:
                    f = float(val)
                except (TypeError, ValueError):
                    continue
                if f == 0:
                    continue
                vals.append(f)
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    median = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
    return {"unit": "mg/100g", "value": median}


def _filter_evidences(
    evidences: list[dict],
    *,
    scores: dict[str, float],
    mode: TrustMode,
    threshold: float,
) -> list[dict]:
    kept: list[dict] = []
    for ev in evidences:
        new_extractions = _filter_extractions(
            ev.get("extraction") or [],
            scores=scores,
            mode=mode,
            threshold=threshold,
        )
        if new_extractions:
            kept_ev = dict(ev)
            kept_ev["extraction"] = new_extractions
            kept.append(kept_ev)
    return kept


def _filter_extractions(
    extractions: list[dict],
    *,
    scores: dict[str, float],
    mode: TrustMode,
    threshold: float,
) -> list[dict]:
    out: list[dict] = []
    for ext in extractions:
        score = scores.get(ext.get("attestation_id", ""), _NO_SIGNAL_DEFAULT_SCORE)
        # Threshold is inclusive on the low side: score <= threshold counts as
        # low-trust. The model often lands on round numbers (e.g. 0.3 for
        # "suspicious but not impossible") — strict `<` would let those slip.
        if (mode == "default" and score > threshold) or (
            mode == "low_only" and score <= threshold
        ):
            out.append(ext)
    return out
