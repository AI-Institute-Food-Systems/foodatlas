"""Per-row assay counts under the page filters, and the unit-folding SQL.

The "N assays" button must equal what the measurements modal shows when it
opens on the page's filters. The modal reads ``/bioactivity/measurements``,
which drops hidden endpoints and folds unit aliases (``_bioact_hotfix``), so
these counts apply exactly the same rules, generated from the same constants.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from . import _bioact_hotfix as hotfix

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def unit_norm_sql(col: str) -> str:
    """SQL twin of ``_bioact_hotfix.normalize_unit`` for column ``col``."""
    folded = " ".join(
        f"WHEN {_sql_str(alias)} THEN {_sql_str(canon)}"
        for alias, canon in sorted(hotfix._UNIT_ALIASES.items())
    )
    trimmed = f"btrim({col})"
    return (
        f"(CASE WHEN {col} IS NULL OR {trimmed} = '' OR upper({trimmed}) = 'NONE'"
        f" THEN 'None' ELSE CASE lower({trimmed}) {folded} ELSE {trimmed} END END)"
    )


def _dropped_endpoint_sql(col: str) -> str:
    """SQL twin of ``_bioact_hotfix.should_drop``; true for hidden endpoints."""
    leaked = ", ".join(_sql_str(s) for s in sorted(hotfix._LEAKED_ASSAY_ENDPOINTS))
    outcome = ", ".join(_sql_str(s) for s in sorted(hotfix._OUTCOME_NOT_ENDPOINT))
    return (
        f"({col} IS NOT NULL AND (btrim({col}) IN ({leaked})"
        f" OR lower(btrim({col})) IN ({outcome})))"
    )


def split_multi(value: str) -> list[str]:
    """A '+'-separated multi-select query value as a list."""
    return [v.strip() for v in value.split("+") if v.strip()]


def measurement_where(
    evidence_types: list[str], units: list[str], source_kind: str
) -> tuple[list[str], dict]:
    """Predicates on ``base_attestations_bioactivity ba``, as the modal applies them."""
    where = [f"NOT {_dropped_endpoint_sql('ba.evidence_endpoint_type')}"]
    params: dict = {}
    if evidence_types:
        where.append("btrim(coalesce(ba.evidence_type, '')) = ANY(:m_ets)")
        params["m_ets"] = evidence_types
    if units:
        where.append(f"{unit_norm_sql('ba.potency_unit')} = ANY(:m_units)")
        params["m_units"] = units
    if source_kind == "experimental":
        where.append("lower(coalesce(ba.evidence_source, '')) LIKE 'exp%'")
    elif source_kind == "predicted":
        where.append(
            "(lower(coalesce(ba.evidence_source, '')) LIKE 'pred%'"
            " OR lower(coalesce(ba.evidence_source, '')) LIKE 'comp%')"
        )
    return where, params


def _key_part(key_col: str, n: int) -> str:
    """Field ``n`` (1-based) of an ``assay_keys`` entry, split on chr(31)."""
    return f"split_part({key_col}, chr(31), {n})"


def key_evidence(key_col: str) -> str:
    return f"btrim({_key_part(key_col, 1)})"


def key_unit(key_col: str) -> str:
    return unit_norm_sql(_key_part(key_col, 2))


def key_conditions(
    key_col: str,
    *,
    evidence_type: str = "",
    unit: str = "",
    source_kind: str = "",
    skip: str = "",
) -> tuple[list[str], dict]:
    """Conditions on one assay key, as the modal applies them to one assay.

    ``skip`` leaves one dimension out, for that dimension's sidebar counts.
    The source kind is inlined: it is one of two fixed literals.
    """
    conds = [f"NOT {_dropped_endpoint_sql(_key_part(key_col, 4))}"]
    params: dict = {}
    ets = split_multi(evidence_type)
    if ets and skip != "evidence":
        conds.append(f"{key_evidence(key_col)} = ANY(:k_ets)")
        params["k_ets"] = ets
    units = split_multi(unit)
    if units and skip != "unit":
        conds.append(f"{key_unit(key_col)} = ANY(:k_units)")
        params["k_units"] = units
    if source_kind in ("experimental", "predicted") and skip != "source":
        conds.append(f"{_key_part(key_col, 3)} = '{source_kind}'")
    return conds, params


def assay_exists_sql(keys_col: str, conds: list[str]) -> str:
    """True when one assay key in ``keys_col`` meets every condition."""
    return (
        f"EXISTS (SELECT 1 FROM unnest({keys_col}) AS k(key)"
        f" WHERE {' AND '.join(conds)})"
    )


async def filtered_counts(
    session: AsyncSession,
    relationship: str,
    pairs: list[tuple[str, str]],
    *,
    evidence_type: str = "",
    unit: str = "",
    source_kind: str = "",
) -> dict[tuple[str, str], int]:
    """Displayed measurements per (head, tail) pair under the given filters."""
    if not pairs:
        return {}
    where, params = measurement_where(
        split_multi(evidence_type), split_multi(unit), source_kind
    )
    result = await session.execute(
        text(f"""
            SELECT bt.head_id, bt.tail_id, COUNT(*) AS n
            FROM base_triplets bt
            CROSS JOIN LATERAL unnest(bt.attestation_ids) AS att(bm)
            JOIN base_attestations_bioactivity ba
              ON ba.bioactivity_metadata_id = att.bm
            WHERE bt.relationship_id = :rel
              AND (bt.head_id, bt.tail_id) IN (
                  SELECT * FROM unnest(CAST(:heads AS TEXT[]), CAST(:tails AS TEXT[]))
              )
              AND {" AND ".join(where)}
            GROUP BY bt.head_id, bt.tail_id
        """),
        {
            "rel": relationship,
            "heads": [h for h, _ in pairs],
            "tails": [t for _, t in pairs],
            **params,
        },
    )
    return {(r.head_id, r.tail_id): int(r.n) for r in result}
