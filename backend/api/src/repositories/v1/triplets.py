"""Graph-primitive queries for /v1/ (triplets + attestations).

These expose the raw knowledge graph: every triplet is a (head, relationship,
tail) edge, every attestation is one piece of evidence backing one triplet.
This is the surface a researcher building over the KG actually wants —
flat rows, stable ids, no UI-level aggregation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from .pagination import offset as _offset

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_VALID_RELATIONSHIPS = {"r1", "r2", "r3", "r4", "r5"}
_RELATIONSHIP_ALIASES = {
    "contains": "r1",
    "is_a": "r2",
    "worsens": "r3",
    "reduces": "r4",
}


def _resolve_relationship(value: str) -> str | None:
    if not value:
        return None
    if value in _VALID_RELATIONSHIPS:
        return value
    return _RELATIONSHIP_ALIASES.get(value.lower())


async def list_triplets(
    session: AsyncSession,
    *,
    head_id: str = "",
    tail_id: str = "",
    relationship: str = "",
    source: str = "",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List triplets, filtered by any combination of head/tail/rel/source."""
    where: list[str] = []
    params: dict[str, object] = {}
    if head_id:
        where.append("t.head_id = :head_id")
        params["head_id"] = head_id
    if tail_id:
        where.append("t.tail_id = :tail_id")
        params["tail_id"] = tail_id
    rel_id = _resolve_relationship(relationship) if relationship else None
    if relationship and rel_id is None:
        return [], 0
    if rel_id:
        where.append("t.relationship_id = :rel_id")
        params["rel_id"] = rel_id
    if source:
        where.append("t.source = :source")
        params["source"] = source
    where_sql = " WHERE " + " AND ".join(where) if where else ""

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM base_triplets t{where_sql}"), params
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = f"""
        SELECT
            t.triplet_id,
            t.head_id,
            h.common_name AS head_name,
            t.relationship_id,
            r.name AS relationship_name,
            t.tail_id,
            tl.common_name AS tail_name,
            t.source,
            t.attestation_ids
        FROM base_triplets t
        JOIN base_entities h  ON h.foodatlas_id = t.head_id
        JOIN base_entities tl ON tl.foodatlas_id = t.tail_id
        JOIN relationships r  ON r.foodatlas_id = t.relationship_id
        {where_sql}
        ORDER BY t.triplet_id
        OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
    """
    rows_result = await session.execute(text(sql), params)
    rows = [dict(r._mapping) for r in rows_result]

    if rows:
        att_summaries = await _attestation_summaries(
            session,
            {aid for row in rows for aid in (row.get("attestation_ids") or [])},
        )
        for row in rows:
            row["attestations"] = [
                att_summaries[aid]
                for aid in (row.pop("attestation_ids", []) or [])
                if aid in att_summaries
            ]
    else:
        for row in rows:
            row["attestations"] = []
    return rows, total


async def get_triplet(session: AsyncSession, triplet_id: int) -> dict | None:
    """Return a single triplet with attestation summaries."""
    sql = """
        SELECT
            t.triplet_id,
            t.head_id,
            h.common_name AS head_name,
            t.relationship_id,
            r.name AS relationship_name,
            t.tail_id,
            tl.common_name AS tail_name,
            t.source,
            t.attestation_ids
        FROM base_triplets t
        JOIN base_entities h  ON h.foodatlas_id = t.head_id
        JOIN base_entities tl ON tl.foodatlas_id = t.tail_id
        JOIN relationships r  ON r.foodatlas_id = t.relationship_id
        WHERE t.triplet_id = :tid
    """
    result = await session.execute(text(sql), {"tid": triplet_id})
    row = result.first()
    if row is None:
        return None
    row_dict = dict(row._mapping)
    aids = row_dict.pop("attestation_ids", None) or []
    summaries = await _attestation_summaries(session, set(aids))
    row_dict["attestations"] = [summaries[a] for a in aids if a in summaries]
    return row_dict


async def get_attestation(session: AsyncSession, attestation_id: str) -> dict | None:
    """Return one attestation as a flat dict.

    Joins to ``base_triplets`` to surface (head_id, tail_id, relationship_id)
    since the attestation table itself only has raw names. An attestation
    can back many triplets via ``attestation_ids``; the first matching
    triplet is returned for the head/tail/rel fields. Trust score is the
    latest valid ``llm_plausibility`` row.
    """
    att_sql = """
        SELECT
            a.attestation_id,
            a.source,
            a.evidence_id,
            a.head_name_raw,
            a.tail_name_raw,
            a.conc_value,
            a.conc_unit,
            a.food_part,
            a.food_processing,
            a.validated,
            a.validated_correct
        FROM base_attestations a
        WHERE a.attestation_id = :aid
    """
    result = await session.execute(text(att_sql), {"aid": attestation_id})
    row = result.first()
    if row is None:
        return None
    out = dict(row._mapping)

    trip_sql = """
        SELECT head_id, tail_id, relationship_id
        FROM base_triplets
        WHERE :aid = ANY(attestation_ids)
        LIMIT 1
    """
    trip = (await session.execute(text(trip_sql), {"aid": attestation_id})).first()
    out["head_id"] = trip[0] if trip else ""
    out["tail_id"] = trip[1] if trip else ""
    out["relationship_id"] = trip[2] if trip else ""

    summaries = await _attestation_summaries(session, {attestation_id})
    summary = summaries.get(attestation_id)
    out["trust_score"] = summary["trust_score"] if summary else None
    out["trust_reason"] = await _trust_reason(session, attestation_id) or ""
    return out


async def _attestation_summaries(
    session: AsyncSession, attestation_ids: set[str]
) -> dict[str, dict]:
    """Fetch summary rows + latest trust scores for a set of attestation ids."""
    if not attestation_ids:
        return {}
    rows_result = await session.execute(
        text(
            "SELECT attestation_id, source, evidence_id "
            "FROM base_attestations WHERE attestation_id = ANY(:ids)"
        ),
        {"ids": list(attestation_ids)},
    )
    base = {
        r.attestation_id: {
            "attestation_id": r.attestation_id,
            "source": r.source,
            "evidence_id": r.evidence_id,
            "trust_score": None,
        }
        for r in rows_result
    }
    scores = await _latest_trust_scores(session, list(attestation_ids))
    for aid, score in scores.items():
        if aid in base:
            base[aid]["trust_score"] = score
    return base


async def _latest_trust_scores(
    session: AsyncSession, attestation_ids: list[str]
) -> dict[str, float]:
    if not attestation_ids:
        return {}
    result = await session.execute(
        text("""
            WITH ranked AS (
                SELECT attestation_id, score,
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
        {"ids": attestation_ids},
    )
    return {row.attestation_id: float(row.score) for row in result}


async def _trust_reason(session: AsyncSession, attestation_id: str) -> str | None:
    result = await session.execute(
        text("""
            SELECT reason FROM base_trust_signals
            WHERE attestation_id = :aid
              AND signal_kind = 'llm_plausibility'
              AND score >= 0
            ORDER BY created_at DESC
            LIMIT 1
        """),
        {"aid": attestation_id},
    )
    row = result.first()
    return row[0] if row else None
