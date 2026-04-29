"""Per-attestation trust-score filtering applied to composition responses.

Each evidence dict in the materialised composition view (``fdc_evidences``,
``foodatlas_evidences``, ``dmd_evidences``) carries a list of ``extraction``
objects, each annotated with an ``attestation_id``. Trust signals live in
``base_trust_signals`` and are joined at request time by attestation_id. The
threshold is configurable via :class:`APISettings.trust_low_threshold`.

Modes:

- ``default`` — drop extractions whose latest llm_plausibility score is below
  the threshold; if a row's evidences end up entirely empty, drop the row.
- ``show_all`` — pass through unchanged so the UI can render low-trust rows
  with a warning icon.
- ``low_only`` — keep only extractions whose score is below the threshold;
  drop rows that end up with no evidences. Used when the user clicks the
  warning icon to filter to suspicious data points (mirrors the
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
    """Filter ``rows`` in-place semantics: returns a new list of dicts.

    ``rows`` is a list of MV-shaped dicts (each may contain
    ``fdc_evidences`` / ``foodatlas_evidences`` / ``dmd_evidences`` keys
    with list-or-None values). Every extraction must already carry
    ``attestation_id`` (added in the materializer).
    """
    if mode == "show_all" or not rows:
        return rows

    att_ids = _collect_attestation_ids(rows)
    if not att_ids:
        # Nothing to score; default mode keeps everything (treats as
        # high-trust); low_only mode drops everything since no row has
        # any low-trust evidence.
        return rows if mode == "default" else []

    scores = await _fetch_trust_scores(session, att_ids)
    return _filter_rows(rows, scores=scores, mode=mode, threshold=threshold)


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
    """Latest valid llm_plausibility score per attestation; -1 errors excluded."""
    result = await session.execute(
        text("""
            SELECT attestation_id, MAX(score) AS score
            FROM base_trust_signals
            WHERE attestation_id = ANY(:ids)
              AND signal_kind = 'llm_plausibility'
              AND score >= 0
            GROUP BY attestation_id
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
        if any_kept:
            out.append(new_row)
    return out


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
        if (mode == "default" and score >= threshold) or (
            mode == "low_only" and score < threshold
        ):
            out.append(ext)
    return out
