"""Per-row assay counts under the page filters, and the unit-folding SQL.

The "N assays" button must equal what the measurements modal shows when it
opens on the page's filters. The modal reads ``/bioactivity/measurements``,
which drops hidden endpoints and folds unit aliases (``_bioact_hotfix``), so
the SQL here applies exactly the same rules, generated from the same
constants, over the MV's per-key counts (``assay_key_counts``).
"""

from __future__ import annotations

from . import _bioact_hotfix as hotfix


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


def filtered_count_sql(counts_col: str, conds: list[str]) -> str:
    """Measurements in ``counts_col`` ({assay key: n}) whose key meets ``conds``.

    ``conds`` come from :func:`key_conditions` on ``k.key``. This is the
    modal's total under the page filters: hidden endpoints never count.
    """
    return (
        "(SELECT COALESCE(SUM(k.n::int), 0)"
        f" FROM jsonb_each_text({counts_col}) AS k(key, n)"
        f" WHERE {' AND '.join(conds)})"
    )
