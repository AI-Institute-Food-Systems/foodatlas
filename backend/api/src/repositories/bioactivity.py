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
    # Correlated subquery alias — see get_chemicals.select_cols.
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


def _attach_top_measurement(data: list[dict]) -> list[dict]:
    for row in data:
        row["top_measurement"] = _top_measurement_by_value(row.get("measurements"))
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
    if search:
        where_parts.append(f"{search_col} ILIKE :q")
        params["q"] = f"%{search}%"

    has_filter = bool(filter_endpoint and filter_unit)
    if has_filter:
        where_parts.append(
            "EXISTS (SELECT 1 FROM jsonb_array_elements(measurements) m"
            " WHERE m->>'endpoint' = :ep AND m->>'unit' = :unit)"
        )
        params["ep"] = filter_endpoint
        params["unit"] = filter_unit

    where = " AND ".join(where_parts)

    total_result = await session.execute(
        text(f"SELECT COUNT(*) FROM {mv} WHERE {where}"),
        params,
    )
    total = int(total_result.scalar() or 0)

    # When sorting by top value, materialise it as a SELECT-list
    # expression and ORDER BY it. Falls back gracefully (returns 0 rows
    # via NULLS LAST) when the row has no matching measurement.
    order_expr = sort_col
    extra_select = ""
    if sort_col == TOP_VALUE_SORT:
        if not has_filter:
            # Without an endpoint+unit, top-value sort is undefined; fall
            # back to measurement_count so the page still renders.
            order_expr = "measurement_count"
        else:
            extra_select = (
                ", (SELECT MAX((m->>'value')::NUMERIC)"
                " FROM jsonb_array_elements(measurements) m"
                " WHERE m->>'endpoint' = :ep AND m->>'unit' = :unit"
                ") AS top_measurement_value"
            )
            order_expr = "top_measurement_value"

    offset = rows_per_page * (page - 1)
    rows_result = await session.execute(
        text(f"""
            SELECT {select_cols}{extra_select}
            FROM {mv} WHERE {where}
            ORDER BY {order_expr} {sort_dir} NULLS LAST
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {**params, "offset": offset, "limit": rows_per_page},
    )
    data = _attach_top_measurement([dict(r._mapping) for r in rows_result])
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
) -> dict[str, object]:
    """Chemicals measured for this bioactivity."""
    sort_col, direction = _resolve_sort(
        sort_by, sort_dir, _BIO_CHEM_SORT, "measurement_count"
    )
    return await _paginated(
        session,
        mv="mv_chemical_bioactivity",
        name_col="bioactivity_name",
        bind_value=common_name,
        select_cols=(
            "chemical_name AS name, chemical_foodatlas_id AS id, "
            "measurement_count, active_count, inactive_count, "
            "unspecified_count, inconclusive_count, measurements, "
            # TODO(perf): switch to the materialised n_foods column once
            # the next ETL run lands (model + materializer added the
            # column in the same PR; column-access avoids the per-row
            # subquery cost entirely). Until then, this correlated
            # subquery is index-served by ix_mv_fcc_chemical_id and
            # already fast enough.
            "(SELECT COUNT(DISTINCT food_foodatlas_id) "
            "FROM mv_food_chemical_composition "
            "WHERE chemical_foodatlas_id = "
            "mv_chemical_bioactivity.chemical_foodatlas_id) AS n_foods"
        ),
        search_col="chemical_name",
        search=search,
        sort_col=sort_col,
        sort_dir=direction,
        page=page,
        rows_per_page=rows_per_page,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
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
}


async def get_food_inferred_bioactivities(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    search: str = "",
    sort_by: str = "concentration",
    sort_dir: str = "desc",
    rows_per_page: int = ROWS_PER_PAGE_DEFAULT,
) -> dict[str, object]:
    """Bioactivities of the chemicals found in this food (transitive)."""
    sort_col = _INFERRED_SORT.get(sort_by, _INFERRED_SORT["concentration"])
    direction = sort_dir.upper() if sort_dir.upper() in _VALID_DIR else "DESC"

    params: dict = {"name": common_name}
    where_parts = ["fcc.food_name = :name"]
    if search:
        where_parts.append(
            "(cb.bioactivity_name ILIKE :q OR fcc.chemical_name ILIKE :q)"
        )
        params["q"] = f"%{search}%"
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
              cb.measurements
            FROM mv_food_chemical_composition fcc
            JOIN mv_chemical_bioactivity cb
              ON cb.chemical_foodatlas_id = fcc.chemical_foodatlas_id
            WHERE {where}
            ORDER BY {sort_col} {direction} NULLS LAST
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {**params, "offset": offset, "limit": rows_per_page},
    )
    data = _attach_top_measurement([dict(r._mapping) for r in rows_result])
    return {
        "data": data,
        "metadata": _build_meta(total, page, rows_per_page, len(data)),
    }


# ---------------------------------------------------------------------
# Distinct (endpoint, unit) combos for one bioactivity ↔ {chemical|food}
# direction — used by the table UI to populate filter chips. Counts come
# from the bioactivity attestation store (no MV cap) so they reflect the
# full set, not just the 25-row sample carried on each MV row.
# ---------------------------------------------------------------------


# direction → (relationship_id, pivot_side, pivot MV, pivot name column).
# pivot_side is "head_id" or "tail_id" — the triplet column that holds
# the pivot entity's id. The OPPOSITE side varies across rows (it's the
# table-row entity), which is why the filter scopes only the pivot side.
_DIRECTION_PIVOTS: dict[str, tuple[str, str, str]] = {
    "bioactivity-chemicals": ("r6", "tail_id", "mv_bioactivity_entities"),
    "bioactivity-foods": ("r5", "tail_id", "mv_bioactivity_entities"),
    "chemical-bioactivities": ("r6", "head_id", "mv_chemical_entities"),
    "food-bioactivities": ("r5", "head_id", "mv_food_entities"),
}


async def get_endpoint_options(
    session: AsyncSession, common_name: str, direction: str
) -> dict[str, object]:
    """List distinct (endpoint, unit) combinations for the given pivot.

    Used to populate the table's endpoint+unit filter chip row. Counts
    are descending by occurrence so the UI can surface the most useful
    chips first.
    """
    info = _DIRECTION_PIVOTS.get(direction)
    if info is None:
        return {"data": [], "metadata": {"row_count": 0}}
    rel, pivot_side, mv = info

    pivot_id = (
        await session.execute(
            text(f"SELECT foodatlas_id FROM {mv} WHERE common_name = :name"),
            {"name": common_name},
        )
    ).scalar()
    if not pivot_id:
        return {"data": [], "metadata": {"row_count": 0}}

    rows_result = await session.execute(
        text(f"""
            SELECT ba.evidence_endpoint_type AS endpoint,
                   ba.potency_unit AS unit,
                   COUNT(*) AS count
            FROM base_triplets bt
            CROSS JOIN LATERAL unnest(bt.attestation_ids) AS att(bm)
            JOIN base_attestations_bioactivity ba
              ON ba.bioactivity_metadata_id = att.bm
            WHERE bt.relationship_id = :rel
              AND bt.{pivot_side} = :pivot
              AND ba.evidence_endpoint_type <> ''
              AND ba.potency_unit <> ''
            GROUP BY ba.evidence_endpoint_type, ba.potency_unit
            ORDER BY COUNT(*) DESC
        """),
        {"rel": rel, "pivot": pivot_id},
    )
    data = [dict(r._mapping) for r in rows_result]
    return {"data": data, "metadata": {"row_count": len(data)}}


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
    rows = [dict(r._mapping) for r in rows_result]

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
