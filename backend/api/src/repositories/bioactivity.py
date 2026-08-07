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
) -> list[dict]:
    for row in data:
        # HOTFIX 2026-06-26 — clean dirty endpoint/unit values in the
        # inline measurements sample before downstream consumers see
        # them. See _bioact_hotfix.py for the rules + removal note.
        cleaned = _bioact_hotfix.clean_measurements(row.get("measurements"))
        # When the user has an evidence-type filter active, narrow the
        # sample so the per-row top_measurement (and modal preview)
        # reflect the filtered set rather than the union. Row presence
        # itself is already filtered in the SQL via EXISTS, so this is
        # purely display.
        if evidence_type:
            wanted = {t.strip() for t in evidence_type.split("+") if t.strip()}
            if wanted:
                cleaned = [
                    m for m in cleaned if (m.get("evidence_type") or "") in wanted
                ]
        # Same treatment for the single-select Assay Source filter:
        # keep only measurements matching the selected kind so the
        # displayed "N assays" (and top_measurement, and the modal
        # preview) reflect that subset. Also rewrite the row's
        # measurement_count so the "Assays (experimental)" column
        # renders the filtered number instead of the total.
        if source_kind in ("experimental", "predicted"):
            cleaned = [m for m in cleaned if _source_kind_of(m) == source_kind]
            row["measurement_count"] = len(cleaned)
        row["measurements"] = cleaned
        row["top_measurement"] = _top_measurement_by_value(cleaned)
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


def _apply_endpoint_unit_filter(
    where_parts: list[str],
    params: dict,
    filter_endpoint: str,
    filter_unit: str,
    has_pair: bool,
    measurements_col: str = "measurements",
) -> None:
    """Endpoint+unit strict pair OR unit-only multi-select."""
    if has_pair:
        where_parts.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements({measurements_col}) m"
            " WHERE m->>'endpoint' = :ep AND m->>'unit' = :unit)"
        )
        params["ep"] = filter_endpoint
        params["unit"] = filter_unit
        return
    if not filter_unit:
        return
    units = [u for u in filter_unit.split("+") if u]
    if not units:
        return
    where_parts.append(
        f"EXISTS (SELECT 1 FROM jsonb_array_elements({measurements_col}) m"
        " WHERE m->>'unit' = ANY(:units))"
    )
    params["units"] = units


def _apply_evidence_type_filter(
    where_parts: list[str],
    params: dict,
    filter_evidence_type: str,
    measurements_col: str = "measurements",
) -> None:
    """Multi-select evidence-type filter ('+'-separated).

    Row qualifies when its measurements sample carries at least one
    entry whose ``evidence_type`` is in the selected set (e.g.
    ``in vitro+in vivo``). A single value still works (one-element set).
    """
    if not filter_evidence_type:
        return
    types = [t.strip() for t in filter_evidence_type.split("+") if t.strip()]
    if not types:
        return
    where_parts.append(
        f"EXISTS (SELECT 1 FROM jsonb_array_elements({measurements_col}) m"
        " WHERE m->>'evidence_type' = ANY(:ets))"
    )
    params["ets"] = types


def _apply_source_kind_filter(
    where_parts: list[str],
    filter_source_kind: str,
    measurements_col: str,
) -> None:
    """Single-select provenance filter.

    ``"experimental"`` keeps rows with AT LEAST ONE experimental
    measurement (source LIKE 'exp%'); ``"predicted"`` keeps rows with
    at least one predicted/computational measurement
    (source LIKE 'pred%'/'comp%'). Any other value — most importantly
    ``"both"`` and the empty string (the frontend default) — is a
    no-op, i.e. no filter applied.

    Row exclusion at the SQL level is by EXISTS over the capped
    measurements sample; the per-row count returned to the UI is
    narrowed to the same subset in ``_attach_top_measurement`` so the
    "Assays (experimental)" column reads consistently.
    """
    if filter_source_kind == "experimental":
        where_parts.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements({measurements_col}) m"
            " WHERE lower(m->>'evidence_source') LIKE 'exp%')"
        )
    elif filter_source_kind == "predicted":
        where_parts.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements({measurements_col}) m"
            " WHERE lower(m->>'evidence_source') LIKE 'pred%'"
            " OR lower(m->>'evidence_source') LIKE 'comp%')"
        )


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
    _apply_endpoint_unit_filter(
        where_parts, params, filter_endpoint, filter_unit, has_filter
    )
    _apply_evidence_type_filter(where_parts, params, filter_evidence_type)
    _apply_source_kind_filter(where_parts, filter_source_kind, "measurements")
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
    data = _attach_top_measurement(
        [dict(r._mapping) for r in rows_result],
        evidence_type=filter_evidence_type,
        source_kind=filter_source_kind,
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
    direction = sort_dir.upper() if sort_dir.upper() in _VALID_DIR else "DESC"
    # Every column carries its own direction + NULLS LAST; a bare
    # `ORDER BY a, b DESC` would silently sort `a` ascending.
    sort_cols = [sort_col] if isinstance(sort_col, str) else sort_col
    order_by = ", ".join(f"{c} {direction} NULLS LAST" for c in sort_cols)

    params: dict = {"name": common_name}
    where_parts = ["fcc.food_name = :name"]
    search_pattern = build_ilike_pattern(search)
    if search_pattern:
        where_parts.append(
            "(cb.bioactivity_name ILIKE :q OR fcc.chemical_name ILIKE :q)"
        )
        params["q"] = search_pattern
    _apply_source_kind_filter(where_parts, filter_source_kind, "cb.measurements")
    _apply_evidence_type_filter(
        where_parts, params, filter_evidence_type, "cb.measurements"
    )
    _apply_endpoint_unit_filter(
        where_parts,
        params,
        filter_endpoint="",
        filter_unit=filter_unit,
        has_pair=False,
        measurements_col="cb.measurements",
    )
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
              cb.measurements,
              eff.efficacy_fraction,
              eff.conc_vs_ac50,
              eff.dose_over_ac50_log,
              eff.n_curves
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
    data = _attach_top_measurement(
        [dict(r._mapping) for r in rows_result],
        evidence_type=filter_evidence_type,
        source_kind=filter_source_kind,
    )
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

    Special direction: "food-inferred-bioactivities" — walks the food's
    chemistry (mv_food_chemical_composition) and surfaces the units from
    every chemical's measurements. Used by the food page's shared
    bioactivities sidebar so its unit list isn't limited to the direct
    (food-level) measurements alone.

    TODO: this endpoint should also become faceted (accept filter_category,
    filter_source_kind, search) and return per-row unit counts so the
    Unit chip count stays in sync when other filters are applied. Blocked
    on rewriting the attestation-based aggregate as an MV-based row-count
    query without regressing the completeness of the unit list.
    """
    if direction == "food-inferred-bioactivities":
        rows_result = await session.execute(
            text("""
                SELECT ba.evidence_endpoint_type AS endpoint,
                       ba.potency_unit AS unit,
                       COUNT(*) AS count
                FROM mv_food_chemical_composition fcc
                JOIN base_triplets bt
                  ON bt.head_id = fcc.chemical_foodatlas_id
                 AND bt.relationship_id = 'r6'
                CROSS JOIN LATERAL unnest(bt.attestation_ids) AS att(bm)
                JOIN base_attestations_bioactivity ba
                  ON ba.bioactivity_metadata_id = att.bm
                WHERE fcc.food_name = :name
                  AND ba.evidence_endpoint_type <> ''
                  AND ba.potency_unit <> ''
                GROUP BY ba.evidence_endpoint_type, ba.potency_unit
                ORDER BY COUNT(*) DESC
            """),
            {"name": common_name},
        )
        data = _bioact_hotfix.clean_endpoint_options(
            [dict(r._mapping) for r in rows_result]
        )
        return {"data": data, "metadata": {"row_count": len(data)}}

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
    # HOTFIX 2026-06-26 — drop leaked-assay/outcome endpoints and fold
    # unit aliases before exposing the chip list. See _bioact_hotfix.py.
    data = _bioact_hotfix.clean_endpoint_options(
        [dict(r._mapping) for r in rows_result]
    )
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_category_options(
    session: AsyncSession,
    common_name: str,
    filter_unit: str = "",
    filter_source_kind: str = "",
    search: str = "",
) -> dict[str, object]:
    """Global chemical-classification counts for the bioactivity-chemicals
    direction — the sidebar's Category filter shows counts across ALL
    matching chemicals with every other active filter (unit, source
    kind, search) applied so the counts stay in sync with the table.
    """
    extras, extra_params = _sidebar_extra_where(
        mv_alias="mv",
        filter_unit=filter_unit,
        filter_source_kind=filter_source_kind,
        search=search,
        search_col="mv.chemical_name",
        include_category=False,
    )
    where = ["mv.bioactivity_name = :name", "category <> ''", *extras]
    rows_result = await session.execute(
        text(f"""
            SELECT category, COUNT(*) AS count
            FROM mv_chemical_bioactivity mv
            JOIN mv_chemical_entities ce
              ON ce.foodatlas_id = mv.chemical_foodatlas_id
            CROSS JOIN LATERAL UNNEST(ce.chemical_classification) AS category
            WHERE {" AND ".join(where)}
            GROUP BY category
            ORDER BY COUNT(*) DESC
        """),
        {"name": common_name, **extra_params},
    )
    data = [dict(r._mapping) for r in rows_result]
    return {"data": data, "metadata": {"row_count": len(data)}}


# Per-direction (mv, name_col) mapping for the source-kind counts
# endpoint. Same MVs the paginated queries hit, filtered on the same
# pivot column, so the counts match what the table would return with
# the corresponding source-kind filter applied.
_SOURCE_KIND_DIRECTIONS: dict[str, tuple[str, str]] = {
    "bioactivity-chemicals": ("mv_chemical_bioactivity", "bioactivity_name"),
    "bioactivity-foods": ("mv_food_bioactivity", "bioactivity_name"),
    "chemical-bioactivities": ("mv_chemical_bioactivity", "chemical_name"),
    "food-bioactivities": ("mv_food_bioactivity", "food_name"),
}

# Column the sidebar search filter applies to for each direction —
# the row-facing name, i.e. what the paginated query treats as the
# table's chemical/food/bioactivity label.
_SEARCH_COL_BY_DIRECTION: dict[str, str] = {
    "bioactivity-chemicals": "mv.chemical_name",
    "bioactivity-foods": "mv.food_name",
    "chemical-bioactivities": "mv.bioactivity_name",
    "food-bioactivities": "mv.bioactivity_name",
}


def _search_col_for(direction: str) -> str:
    return _SEARCH_COL_BY_DIRECTION.get(direction, "")


def _sidebar_extra_where(
    *,
    mv_alias: str,
    filter_unit: str = "",
    filter_category: str = "",
    filter_source_kind: str = "",
    search: str = "",
    search_col: str = "",
    include_unit: bool = True,
    include_category: bool = True,
    include_source_kind: bool = True,
) -> tuple[list[str], dict]:
    """Build the sidebar-count WHERE fragments applying every OTHER filter.

    Callers pass ``include_<dim>=False`` for whichever dimension they're
    counting — that dimension's own filter is skipped so each row count
    reflects "what would this bucket have if I picked it right now?".
    Filters mirror _apply_*_filter helpers used by the paginated query.
    """
    where: list[str] = []
    params: dict = {}
    if include_unit and filter_unit:
        units = [u for u in filter_unit.split("+") if u]
        if units:
            where.append(
                f"EXISTS (SELECT 1 FROM jsonb_array_elements({mv_alias}.measurements) m"
                " WHERE m->>'unit' = ANY(:units))"
            )
            params["units"] = units
    if include_category and filter_category:
        cats = [c for c in filter_category.split("+") if c]
        if cats:
            where.append(
                "EXISTS (SELECT 1 FROM mv_chemical_entities ce"
                f" WHERE ce.foodatlas_id = {mv_alias}.chemical_foodatlas_id"
                " AND ce.chemical_classification && :cats)"
            )
            params["cats"] = cats
    if include_source_kind and filter_source_kind == "experimental":
        where.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements({mv_alias}.measurements) m"
            " WHERE (m->>'evidence_source') ILIKE 'exp%')"
        )
    elif include_source_kind and filter_source_kind == "predicted":
        where.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements({mv_alias}.measurements) m"
            " WHERE (m->>'evidence_source') ILIKE 'pred%'"
            " OR (m->>'evidence_source') ILIKE 'comp%')"
        )
    if search_col:
        search_pattern = build_ilike_pattern(search)
        if search_pattern:
            where.append(f"{search_col} ILIKE :sq")
            params["sq"] = search_pattern
    return where, params


async def get_source_kind_counts(
    session: AsyncSession,
    common_name: str,
    direction: str,
    filter_unit: str = "",
    filter_category: str = "",
    search: str = "",
) -> dict[str, object]:
    """Row counts per assay-source kind for the sidebar Assay Source filter.

    Every other active filter (unit, category, search) is applied so
    the counts stay in sync with what the table would render under
    each source-kind selection.
    """
    if direction == "food-inferred-bioactivities":
        extras, extra_params = _sidebar_extra_where(
            mv_alias="cb",
            filter_unit=filter_unit,
            filter_category=filter_category,
            search=search,
            search_col="(cb.bioactivity_name || ' ' || fcc.chemical_name)",
            include_source_kind=False,
        )
        where = ["fcc.food_name = :name", *extras]
        result = await session.execute(
            text(f"""
                SELECT
                    COUNT(*) AS both,
                    COUNT(*) FILTER (
                        WHERE EXISTS (
                            SELECT 1 FROM jsonb_array_elements(cb.measurements) m
                            WHERE (m->>'evidence_source') ILIKE 'exp%'
                        )
                    ) AS experimental,
                    COUNT(*) FILTER (
                        WHERE EXISTS (
                            SELECT 1 FROM jsonb_array_elements(cb.measurements) m
                            WHERE (m->>'evidence_source') ILIKE 'pred%'
                               OR (m->>'evidence_source') ILIKE 'comp%'
                        )
                    ) AS predicted
                FROM mv_food_chemical_composition fcc
                JOIN mv_chemical_bioactivity cb
                  ON cb.chemical_foodatlas_id = fcc.chemical_foodatlas_id
                WHERE {" AND ".join(where)}
            """),
            {"name": common_name, **extra_params},
        )
        row = result.first()
        return {
            "data": {
                "both": int(row.both or 0) if row else 0,
                "experimental": int(row.experimental or 0) if row else 0,
                "predicted": int(row.predicted or 0) if row else 0,
            }
        }

    info = _SOURCE_KIND_DIRECTIONS.get(direction)
    if info is None:
        return {"data": {"both": 0, "experimental": 0, "predicted": 0}}
    mv, name_col = info
    # Only bioactivity-chemicals rows carry a chemical_foodatlas_id we
    # can join to mv_chemical_entities for the Category filter; other
    # directions ignore the category dim.
    can_category = direction == "bioactivity-chemicals"
    extras, extra_params = _sidebar_extra_where(
        mv_alias="mv",
        filter_unit=filter_unit,
        filter_category=filter_category if can_category else "",
        search=search,
        search_col=_search_col_for(direction),
        include_source_kind=False,
    )
    where = [f"mv.{name_col} = :name", *extras]
    result = await session.execute(
        text(f"""
            SELECT
                COUNT(*) AS both,
                COUNT(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1 FROM jsonb_array_elements(mv.measurements) m
                        WHERE (m->>'evidence_source') ILIKE 'exp%'
                    )
                ) AS experimental,
                COUNT(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1 FROM jsonb_array_elements(mv.measurements) m
                        WHERE (m->>'evidence_source') ILIKE 'pred%'
                           OR (m->>'evidence_source') ILIKE 'comp%'
                    )
                ) AS predicted
            FROM {mv} mv
            WHERE {" AND ".join(where)}
        """),
        {"name": common_name, **extra_params},
    )
    row = result.first()
    return {
        "data": {
            "both": int(row.both or 0) if row else 0,
            "experimental": int(row.experimental or 0) if row else 0,
            "predicted": int(row.predicted or 0) if row else 0,
        }
    }


async def get_evidence_type_counts(
    session: AsyncSession, common_name: str, direction: str
) -> dict[str, object]:
    """Per-evidence_type row counts for the sidebar Evidence filter.

    Evidence types come from the NPASS-style heuristic column
    (``evidence_type`` on ``base_attestations_bioactivity``): typical
    values are "molecular-level", "in vitro", "in vivo", "adme/tox".
    Counts are how many rows the paginated MV would return that have
    AT LEAST ONE measurement of that evidence type — same semantics as
    ``_apply_evidence_type_filter`` so the chip counts and the filter
    behavior stay in sync.

    ``food-inferred-bioactivities`` uses the same
    ``mv_food_chemical_composition`` ⨝ ``mv_chemical_bioactivity`` join
    the paginated inferred query does so its counts match that row set
    exactly.
    """
    if direction == "food-inferred-bioactivities":
        result = await session.execute(
            text("""
                SELECT evidence_type, COUNT(*) AS count
                FROM (
                    SELECT DISTINCT
                        fcc.food_foodatlas_id,
                        cb.chemical_foodatlas_id,
                        cb.bioactivity_foodatlas_id,
                        m->>'evidence_type' AS evidence_type
                    FROM mv_food_chemical_composition fcc
                    JOIN mv_chemical_bioactivity cb
                      ON cb.chemical_foodatlas_id
                       = fcc.chemical_foodatlas_id,
                    LATERAL jsonb_array_elements(cb.measurements) AS m
                    WHERE fcc.food_name = :name
                ) AS x
                WHERE evidence_type IS NOT NULL AND evidence_type <> ''
                GROUP BY evidence_type
                ORDER BY count DESC
            """),
            {"name": common_name},
        )
    else:
        info = _SOURCE_KIND_DIRECTIONS.get(direction)
        if info is None:
            return {"data": [], "metadata": {"row_count": 0}}
        mv, name_col = info
        result = await session.execute(
            text(f"""
                SELECT evidence_type, COUNT(*) AS count
                FROM (
                    SELECT DISTINCT
                        mv.ctid,
                        m->>'evidence_type' AS evidence_type
                    FROM {mv} AS mv,
                    LATERAL jsonb_array_elements(mv.measurements) AS m
                    WHERE mv.{name_col} = :name
                ) AS x
                WHERE evidence_type IS NOT NULL AND evidence_type <> ''
                GROUP BY evidence_type
                ORDER BY count DESC
            """),
            {"name": common_name},
        )
    data = [dict(r._mapping) for r in result]
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
