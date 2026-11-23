"""Bioactivity repository — queries the bioactivity materialized views.

Mirrors the surface of :mod:`food` composition: each list endpoint accepts
``page`` + ``rows_per_page`` for pagination, ``search`` for ILIKE name
filtering, and ``sort_by`` + ``sort_dir`` for column-sortable headers.
Per-row data continues to include the (MV-capped) ``measurements`` sample
and a Python-computed ``top_measurement`` (max-by-value) used as the
table's headline cell.

The dedicated :func:`get_measurements` endpoint stays for future use —
it bypasses the MV and reads ``base_attestations_bioactivity`` directly
so it can return the FULL unbounded set for a single pair.
"""

import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

# HOTFIX 2026-06-26 — REMOVE WHEN upstream endpoint/unit cleanup lands.
# See _bioact_hotfix.py docstring for the rules + removal checklist.
from src.repositories import _bioact_hotfix
from src.repositories._search_util import build_ilike_pattern
from src.repositories.bioactivity_counts import (
    assay_exists_sql,
    filtered_count_sql,
    key_conditions,
    split_multi,
)

logger = logging.getLogger(__name__)

ROWS_PER_PAGE_DEFAULT = 20

# Sort whitelist per MV — keys are the public sort_by values, values are
# the actual SQL columns. Anything else is silently ignored (falls back to
# the default sort). NULLs are pushed to the end in both directions so
# unmeasured rows don't dominate the first page on a desc sort.
_CHEM_BIO_SORT = {
    "name": "chemical_name",
    "measurement_count": "measurement_count",
    "active_count": "active_count",
    "inactive_count": "inactive_count",
}
_BIO_CHEM_SORT = {
    "name": "chemical_name",
    "measurement_count": "measurement_count",
    "active_count": "active_count",
    "inactive_count": "inactive_count",
    "n_foods": "n_foods",
}
_FOOD_BIO_SORT = {
    "name": "food_name",
    "measurement_count": "measurement_count",
}
_BIO_FOOD_SORT = {
    "name": "food_name",
    "measurement_count": "measurement_count",
}
_VALID_DIR = {"ASC", "DESC"}

# Per-pair counts over ALL measurements, stored by the ETL. The Assay Source
# filter reads these, not the 25-row ``measurements`` sample.
_SOURCE_COUNT_COL = {
    "experimental": "experimental_count",
    "predicted": "predicted_count",
}

# Columns that identify one row of each paginated MV.
_MV_ROW_KEY = {
    "mv_chemical_bioactivity": ("chemical_foodatlas_id", "bioactivity_foodatlas_id"),
    "mv_food_bioactivity": ("food_foodatlas_id", "bioactivity_foodatlas_id"),
}

# Pseudo-column used when sorting by the row's top measurement value.
# Resolves to a SQL expression in _paginated, not an actual MV column —
# only valid when filter_endpoint + filter_unit are both supplied, since
# raw values across endpoints/units aren't comparable.
TOP_VALUE_SORT = "top_measurement_value"


def _resolve_sort(
    sort_by: str, sort_dir: str, allowlist: dict[str, str], default: str
) -> tuple[str, str]:
    direction = sort_dir.upper() if sort_dir.upper() in _VALID_DIR else "DESC"
    if sort_by == TOP_VALUE_SORT:
        return TOP_VALUE_SORT, direction
    return allowlist.get(sort_by, default), direction


def _top_measurement_by_value(measurements: list | None) -> dict | None:
    if not measurements:
        return None
    top = None
    for m in measurements:
        v = m.get("value")
        if v is None:
            continue
        if top is None or v > top["value"]:
            top = {
                "endpoint": m.get("endpoint"),
                "value": v,
                "unit": m.get("unit"),
            }
    return top


def _source_kind_of(measurement: dict) -> str:
    """Classify a single measurement as experimental / predicted / ''."""
    src = (measurement.get("evidence_source") or "").lower()
    if src.startswith("exp"):
        return "experimental"
    if src.startswith("pred") or src.startswith("comp"):
        return "predicted"
    return ""


def _attach_top_measurement(
    data: list[dict],
    evidence_type: str = "",
    source_kind: str = "",
    unit: str = "",
) -> list[dict]:
    """Narrow each row's sample to the page filters, then pick its top value.

    The SQL already filtered row presence; the ETL keeps at least one assay
    per evidence type, source kind and unit in the sample, so narrowing never
    empties a row that has a valued match.
    """
    wanted_ets = set(split_multi(evidence_type))
    wanted_units = set(split_multi(unit))
    for row in data:
        # HOTFIX 2026-06-26 — clean dirty endpoint/unit values in the
        # inline measurements sample before downstream consumers see
        # them. See _bioact_hotfix.py for the rules + removal note.
        cleaned = _bioact_hotfix.clean_measurements(row.get("measurements"))
        if wanted_ets:
            cleaned = [
                m
                for m in cleaned
                if (m.get("evidence_type") or "").strip() in wanted_ets
            ]
        if wanted_units:
            cleaned = [m for m in cleaned if m.get("unit") in wanted_units]
        for col in _SOURCE_COUNT_COL.values():
            row.pop(col, None)
        if source_kind in _SOURCE_COUNT_COL:
            cleaned = [m for m in cleaned if _source_kind_of(m) == source_kind]
        row["measurements"] = cleaned
        row["top_measurement"] = _top_measurement_by_value(cleaned)
    return data


def _assay_count(
    alias: str = "",
    *,
    evidence_type: str = "",
    unit: str = "",
    source_kind: str = "",
) -> tuple[str, dict]:
    """SQL for a row's "N assays" under the page filters, and its params."""
    conds, params = key_conditions(
        "k.key", evidence_type=evidence_type, unit=unit, source_kind=source_kind
    )
    return filtered_count_sql(f"{alias}assay_key_counts", conds), params


def _with_filtered_counts(data: list[dict]) -> list[dict]:
    for row in data:
        row["measurement_count"] = int(row.pop("_filtered_count") or 0)
    return data


def _build_meta(total: int, page: int, rows_per_page: int, row_count: int) -> dict:
    total_pages = (total + rows_per_page - 1) // rows_per_page if total else 0
    offset = rows_per_page * (page - 1)
    return {
        "row_count": row_count,
        "rows_per_page": rows_per_page,
        "current_row": offset + 1 if row_count else 0,
        "current_page": page,
        "total_rows": total,
        "total_pages": total_pages,
    }


async def get_metadata(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Bioactivity concept page: hierarchy + linked food/chemical counts."""
    result = await session.execute(
        text("""
            SELECT common_name, foodatlas_id AS id, synonyms, description,
                   external_ids, parents, children, n_foods, n_chemicals
            FROM mv_bioactivity_entities WHERE common_name = :name
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


def _apply_assay_filter(
    where_parts: list[str],
    params: dict,
    alias: str = "",
    *,
    evidence_type: str = "",
    unit: str = "",
    source_kind: str = "",
) -> None:
    """Keep rows with one shown assay that matches every assay filter.

    Evidence, Unit and Source must hold for the SAME assay, so each row
    has at least one assay in its modal under the page filters. Hidden
    hotfix endpoints never count, so rows that only have those are dropped.
    """
    conds, cond_params = key_conditions(
        "k.key", evidence_type=evidence_type, unit=unit, source_kind=source_kind
    )
    where_parts.append(assay_exists_sql(f"{alias}assay_keys", conds))
    params.update(cond_params)


def _apply_endpoint_pair_filter(
    where_parts: list[str], params: dict, filter_endpoint: str, filter_unit: str
) -> None:
    """Endpoint+unit pair filter on the 25-row sample, for the top-value sort."""
    where_parts.append(
        "EXISTS (SELECT 1 FROM jsonb_array_elements(measurements) m"
        " WHERE m->>'endpoint' = :ep AND m->>'unit' = :unit)"
    )
    params["ep"] = filter_endpoint
    params["unit"] = filter_unit


async def _paginated(
    session: AsyncSession,
    *,
    mv: str,
    name_col: str,
    bind_value: str,
    select_cols: str,
    search_col: str,
    search: str,
    sort_col: str,
    sort_dir: str,
    page: int,
    rows_per_page: int,
    filter_endpoint: str = "",
    filter_unit: str = "",
    filter_evidence_type: str = "",
    filter_source_kind: str = "",
    extra_where: list[str] | None = None,
    extra_params: dict | None = None,
) -> dict[str, object]:
    """Shared paginated query against the bioactivity MVs.

    Counts and selects in two passes so total_pages reflects the filtered
    set. ``bind_value`` is the value bound to the WHERE filter on
    ``name_col`` (e.g. the bioactivity name or the chemical name).

    When ``filter_endpoint`` and ``filter_unit`` are both set, the row set
    is restricted to rows whose ``measurements`` JSON array contains at
    least one item matching both, and the sort pseudo-column
    ``TOP_VALUE_SORT`` resolves to ``MAX(value)`` across the matching
    items. (Sorting on top value without a filter would compare raw
    values across incomparable endpoint/unit combos, which is nonsense.)
    """
    params: dict = {"name": bind_value}
    where_parts = [f"{name_col} = :name"]
    search_pattern = build_ilike_pattern(search)
    if search_pattern:
        where_parts.append(f"{search_col} ILIKE :q")
        params["q"] = search_pattern

    has_filter = bool(filter_endpoint and filter_unit)
    if has_filter:
        _apply_endpoint_pair_filter(where_parts, params, filter_endpoint, filter_unit)
    _apply_assay_filter(
        where_parts,
        params,
        evidence_type=filter_evidence_type,
        unit=filter_unit,
        source_kind=filter_source_kind,
    )
    # Caller-supplied additional WHERE clauses (e.g., the Chemical
    # Category filter injected by get_chemicals).
    if extra_where:
        where_parts.extend(extra_where)
    if extra_params:
        params.update(extra_params)

    where = " AND ".join(where_parts)

    total_result = await session.execute(
        text(f"SELECT COUNT(*) FROM {mv} WHERE {where}"),
        params,
    )
    total = int(total_result.scalar() or 0)

    # The "N assays" column sorts by the count the button shows: shown
    # assays under the page filters, never the stored total.
    count_expr, count_params = _assay_count(
        evidence_type=filter_evidence_type,
        unit=filter_unit,
        source_kind=filter_source_kind,
    )
    params.update(count_params)
    order_expr = sort_col
    if sort_col == "measurement_count":
        order_expr = "_filtered_count"
    extra_select = f", {count_expr} AS _filtered_count"
    # When sorting by top value, materialise it as a SELECT-list
    # expression and ORDER BY it. Falls back gracefully (returns 0 rows
    # via NULLS LAST) when the row has no matching measurement.
    if sort_col == TOP_VALUE_SORT:
        if not has_filter:
            # Without an endpoint+unit, top-value sort is undefined; fall
            # back to measurement_count so the page still renders.
            order_expr = "measurement_count"
        else:
            extra_select += (
                ", (SELECT MAX((m->>'value')::NUMERIC)"
                " FROM jsonb_array_elements(measurements) m"
                " WHERE m->>'endpoint' = :ep AND m->>'unit' = :unit"
                ") AS top_measurement_value"
            )
            order_expr = "top_measurement_value"

    # Allowlisted sort columns are NOT NULL, so NULLS LAST only matters for
    # the computed top value; leaving it off the others lets a backward
    # index scan serve DESC. The row key ends the ORDER BY so ties can't
    # shuffle rows between pages.
    nulls = " NULLS LAST" if order_expr == TOP_VALUE_SORT else ""
    tiebreak = "".join(f", {col} {sort_dir}" for col in _MV_ROW_KEY[mv])
    offset = rows_per_page * (page - 1)
    rows_result = await session.execute(
        text(f"""
            SELECT {select_cols}, experimental_count, predicted_count{extra_select}
            FROM {mv} WHERE {where}
            ORDER BY {order_expr} {sort_dir}{nulls}{tiebreak}
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {**params, "offset": offset, "limit": rows_per_page},
    )
    data = _with_filtered_counts(
        _attach_top_measurement(
            [dict(r._mapping) for r in rows_result],
            evidence_type=filter_evidence_type,
            source_kind=filter_source_kind,
            unit=filter_unit,
        )
    )
    return {
        "data": data,
        "metadata": _build_meta(total, page, rows_per_page, len(data)),
    }


async def get_chemicals(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    search: str = "",
    sort_by: str = "measurement_count",
    sort_dir: str = "desc",
    rows_per_page: int = ROWS_PER_PAGE_DEFAULT,
    filter_endpoint: str = "",
    filter_unit: str = "",
    filter_evidence_type: str = "",
    filter_category: str = "",
    filter_source_kind: str = "",
) -> dict[str, object]:
    """Chemicals measured for this bioactivity."""
    sort_col, direction = _resolve_sort(
        sort_by, sort_dir, _BIO_CHEM_SORT, "measurement_count"
    )
    # Chemical Category filter — multi-select via '+'-separated string.
    # Row survives when ANY selected category overlaps the chemical's
    # classification array (Postgres && array-overlap).
    extra_where: list[str] = []
    extra_params: dict = {}
    if filter_category:
        cats = [c for c in filter_category.split("+") if c]
        if cats:
            extra_where.append(
                "EXISTS (SELECT 1 FROM mv_chemical_entities e "
                "WHERE e.foodatlas_id = chemical_foodatlas_id "
                "AND e.chemical_classification && :cats)"
            )
            extra_params["cats"] = cats
    return await _paginated(
        session,
        mv="mv_chemical_bioactivity",
        name_col="bioactivity_name",
        bind_value=common_name,
        select_cols=(
            "chemical_name AS name, chemical_foodatlas_id AS id, "
            "measurement_count, active_count, inactive_count, "
            "unspecified_count, inconclusive_count, measurements, "
            "n_foods, "
            # Correlated lookup so each chemical row carries its
            # classification (e.g. flavonoid, alkaloid) for the sidebar
            # Category filter + the table's new Category column.
            "(SELECT chemical_classification FROM mv_chemical_entities "
            "WHERE foodatlas_id = chemical_foodatlas_id) "
            "AS chemical_classification"
        ),
        search_col="chemical_name",
        search=search,
        sort_col=sort_col,
        sort_dir=direction,
        page=page,
        rows_per_page=rows_per_page,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
        extra_where=extra_where,
        extra_params=extra_params,
    )


async def get_foods(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    search: str = "",
    sort_by: str = "measurement_count",
    sort_dir: str = "desc",
    rows_per_page: int = ROWS_PER_PAGE_DEFAULT,
    filter_endpoint: str = "",
    filter_unit: str = "",
    filter_evidence_type: str = "",
    filter_source_kind: str = "",
) -> dict[str, object]:
    """Foods that exhibit this bioactivity."""
    sort_col, direction = _resolve_sort(
        sort_by, sort_dir, _BIO_FOOD_SORT, "measurement_count"
    )
    return await _paginated(
        session,
        mv="mv_food_bioactivity",
        name_col="bioactivity_name",
        bind_value=common_name,
        select_cols=(
            "food_name AS name, food_foodatlas_id AS id, "
            "measurement_count, measurements"
        ),
        search_col="food_name",
        search=search,
        sort_col=sort_col,
        sort_dir=direction,
        page=page,
        rows_per_page=rows_per_page,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
    )


async def get_chemical_bioactivities(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    search: str = "",
    sort_by: str = "measurement_count",
    sort_dir: str = "desc",
    rows_per_page: int = ROWS_PER_PAGE_DEFAULT,
    filter_endpoint: str = "",
    filter_unit: str = "",
    filter_evidence_type: str = "",
    filter_source_kind: str = "",
) -> dict[str, object]:
    """A chemical's bioactivities."""
    sort_col, direction = _resolve_sort(
        sort_by, sort_dir, _CHEM_BIO_SORT, "measurement_count"
    )
    return await _paginated(
        session,
        mv="mv_chemical_bioactivity",
        name_col="chemical_name",
        bind_value=common_name,
        select_cols=(
            "bioactivity_name AS name, bioactivity_foodatlas_id AS id, "
            "measurement_count, active_count, inactive_count, "
            "unspecified_count, inconclusive_count, measurements"
        ),
        search_col="bioactivity_name",
        search=search,
        sort_col=sort_col,
        sort_dir=direction,
        page=page,
        rows_per_page=rows_per_page,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
    )


async def get_food_bioactivities(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    search: str = "",
    sort_by: str = "measurement_count",
    sort_dir: str = "desc",
    rows_per_page: int = ROWS_PER_PAGE_DEFAULT,
    filter_endpoint: str = "",
    filter_unit: str = "",
    filter_evidence_type: str = "",
    filter_source_kind: str = "",
) -> dict[str, object]:
    """A food's bioactivities."""
    sort_col, direction = _resolve_sort(
        sort_by, sort_dir, _FOOD_BIO_SORT, "measurement_count"
    )
    return await _paginated(
        session,
        mv="mv_food_bioactivity",
        name_col="food_name",
        bind_value=common_name,
        select_cols=(
            "bioactivity_name AS name, bioactivity_foodatlas_id AS id, "
            "measurement_count, measurements"
        ),
        search_col="bioactivity_name",
        search=search,
        sort_col=sort_col,
        sort_dir=direction,
        page=page,
        rows_per_page=rows_per_page,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
    )


# ---------------------------------------------------------------------
# Food → chemical → bioactivity (transitive inference)
# ---------------------------------------------------------------------
# Each row is one (chemical-in-food x bioactivity-of-chemical) pair --
# the chemical comes from mv_food_chemical_composition (carries the
# food-level concentration), the bioactivity counts/sample come from
# mv_chemical_bioactivity (the same source the direct chemical table
# uses). This is presented under the direct food→bioactivity table as
# "implied by the food's chemistry".

_INFERRED_SORT = {
    "bioactivity": "cb.bioactivity_name",
    "chemical": "fcc.chemical_name",
    "concentration": "(fcc.median_concentration->>'value')::NUMERIC",
    "measurement_count": "cb.measurement_count",
    "active_count": "cb.active_count",
    "inactive_count": "cb.inactive_count",
    # efficacy_fraction saturates — roughly half a food's rows sit above 0.99
    # and all render as ">99%" — so ties fall through to dose_over_ac50_log,
    # which still separates them by orders of magnitude. Mirrors the frontend
    # comparator this replaces. Multi-column: the direction has to be repeated
    # per column, since `a, b DESC` would sort `a` ascending.
    "efficacy": ["eff.efficacy_fraction", "eff.dose_over_ac50_log"],
    "n_curves": ["eff.n_curves"],
}

# Efficacy columns LEFT JOINed onto the inferred rows. The join key is food
# plus chemical plus bioactivity, which is unique in mv_food_chemical_efficacy,
# so it can never multiply rows — the COUNT query deliberately omits the join.
# Rows with no Hill fit come back NULL and render as an em-dash.
_INFERRED_EFFICACY_JOIN = """
            LEFT JOIN mv_food_chemical_efficacy eff
              ON eff.food_name = fcc.food_name
             AND eff.chemical_foodatlas_id = fcc.chemical_foodatlas_id
             AND eff.bioactivity_foodatlas_id = cb.bioactivity_foodatlas_id
"""


async def get_food_inferred_bioactivities(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    search: str = "",
    sort_by: str = "concentration",
    sort_dir: str = "desc",
    rows_per_page: int = ROWS_PER_PAGE_DEFAULT,
    filter_source_kind: str = "",
    filter_unit: str = "",
    filter_evidence_type: str = "",
) -> dict[str, object]:
    """Bioactivities of the chemicals found in this food (transitive)."""
    sort_col = _INFERRED_SORT.get(sort_by, _INFERRED_SORT["concentration"])
    count_expr, count_params = _assay_count(
        "cb.",
        evidence_type=filter_evidence_type,
        unit=filter_unit,
        source_kind=filter_source_kind,
    )
    if sort_col == _INFERRED_SORT["measurement_count"]:
        sort_col = count_expr
    direction = sort_dir.upper() if sort_dir.upper() in _VALID_DIR else "DESC"
    # Every column carries its own direction + NULLS LAST; a bare
    # `ORDER BY a, b DESC` would silently sort `a` ascending.
    sort_cols = [sort_col] if isinstance(sort_col, str) else sort_col
    order_by = ", ".join(f"{c} {direction} NULLS LAST" for c in sort_cols)
    # The row key, so ties can't shuffle rows between pages.
    order_by += (
        ", fcc.food_foodatlas_id, fcc.chemical_foodatlas_id,"
        " cb.bioactivity_foodatlas_id"
    )

    params: dict = {"name": common_name}
    where_parts = ["fcc.food_name = :name"]
    search_pattern = build_ilike_pattern(search)
    if search_pattern:
        where_parts.append(
            "(cb.bioactivity_name ILIKE :q OR fcc.chemical_name ILIKE :q)"
        )
        params["q"] = search_pattern
    _apply_assay_filter(
        where_parts,
        params,
        "cb.",
        evidence_type=filter_evidence_type,
        unit=filter_unit,
        source_kind=filter_source_kind,
    )
    params.update(count_params)
    where = " AND ".join(where_parts)

    total_result = await session.execute(
        text(f"""
            SELECT COUNT(*)
            FROM mv_food_chemical_composition fcc
            JOIN mv_chemical_bioactivity cb
              ON cb.chemical_foodatlas_id = fcc.chemical_foodatlas_id
            WHERE {where}
        """),
        params,
    )
    total = int(total_result.scalar() or 0)

    offset = rows_per_page * (page - 1)
    rows_result = await session.execute(
        text(f"""
            SELECT
              cb.bioactivity_name AS bioactivity,
              cb.bioactivity_foodatlas_id AS bioactivity_id,
              fcc.chemical_name AS chemical,
              fcc.chemical_foodatlas_id AS chemical_id,
              fcc.median_concentration,
              cb.measurement_count, cb.active_count, cb.inactive_count,
              cb.experimental_count, cb.predicted_count,
              cb.measurements,
              {count_expr} AS _filtered_count,
              eff.efficacy_fraction,
              eff.conc_vs_ac50,
              eff.dose_over_ac50_log,
              eff.n_curves,
              eff.conc_quality_flag
            FROM mv_food_chemical_composition fcc
            JOIN mv_chemical_bioactivity cb
              ON cb.chemical_foodatlas_id = fcc.chemical_foodatlas_id
            {_INFERRED_EFFICACY_JOIN}
            WHERE {where}
            ORDER BY {order_by}
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {**params, "offset": offset, "limit": rows_per_page},
    )
    data = _with_filtered_counts(
        _attach_top_measurement(
            [dict(r._mapping) for r in rows_result],
            evidence_type=filter_evidence_type,
            source_kind=filter_source_kind,
            unit=filter_unit,
        )
    )
    return {
        "data": data,
        "metadata": _build_meta(total, page, rows_per_page, len(data)),
    }


# ---------------------------------------------------------------------
# Unbounded measurements endpoint (no MV cap)
# ---------------------------------------------------------------------

_MEASUREMENT_COLS = (
    "ba.bioactivity_metadata_id, ba.exhibit_type, "
    "ba.source_assay_id AS assay, ba.reported_activity_outcome AS outcome, "
    "ba.evidence_endpoint_type AS endpoint, ba.evidence_relation AS relation, "
    "ba.potency_value AS value, ba.potency_unit AS unit, "
    "ba.efficacy_zeroactivity, ba.efficacy_infiniteactivity, "
    "ba.efficacy_logac50_value, ba.efficacy_hillslope, "
    "ba.evidence_source, ba.evidence_type, ba.evidence_fit_r2, "
    "ba.evidence_fit_curveclass"
)


async def get_measurements(
    session: AsyncSession,
    head_id: str,
    tail_id: str,
    relationship: str,
) -> dict[str, object]:
    """All measurements for a (head, bioactivity-tail) pair."""
    if relationship not in {"r5", "r6"}:
        return {"data": [], "metadata": {"row_count": 0}}

    rows_result = await session.execute(
        text(f"""
            SELECT {_MEASUREMENT_COLS}
            FROM base_triplets bt
            CROSS JOIN LATERAL unnest(bt.attestation_ids) AS att(bm)
            JOIN base_attestations_bioactivity ba
              ON ba.bioactivity_metadata_id = att.bm
            WHERE bt.relationship_id = :rel
              AND bt.head_id = :head
              AND bt.tail_id = :tail
        """),
        {"rel": relationship, "head": head_id, "tail": tail_id},
    )
    # HOTFIX 2026-06-26 — clean dirty endpoint/unit values before
    # enrichment + return. See _bioact_hotfix.py for rules + removal.
    rows = _bioact_hotfix.clean_measurements([dict(r._mapping) for r in rows_result])

    # Assay metadata is best-effort enrichment — base_bioassays may not exist
    # in environments that haven't yet migrated. Log and continue so the
    # measurements themselves still ship.
    ids = {r["assay"] for r in rows if r.get("assay")}
    if ids:
        try:
            meta_result = await session.execute(
                text("""
                    SELECT source_assay_id, source, assay_description,
                           target_name, target_organism, target_uniprot,
                           target_entrez_gene, n_measurements
                    FROM base_bioassays
                    WHERE source_assay_id = ANY(:ids)
                """),
                {"ids": list(ids)},
            )
            assay_meta = {
                r._mapping["source_assay_id"]: {
                    "source": r._mapping["source"],
                    "description": r._mapping["assay_description"],
                    "target_name": r._mapping["target_name"],
                    "target_organism": r._mapping["target_organism"],
                    "target_uniprot": r._mapping["target_uniprot"],
                    "target_entrez_gene": r._mapping["target_entrez_gene"],
                    "n_measurements": r._mapping["n_measurements"],
                }
                for r in meta_result
            }
            for r in rows:
                r["assay_meta"] = assay_meta.get(r.get("assay"))
        except SQLAlchemyError as exc:
            logger.warning("base_bioassays lookup failed: %s", exc)

    return {"data": rows, "metadata": {"row_count": len(rows)}}
