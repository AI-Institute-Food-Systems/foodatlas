"""Food entity repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import APISettings

from .formatting import format_external_ids
from .trust_filter import TrustMode, apply_trust_filter

ROWS_PER_PAGE = 25

NUTRIENT_KEY_MAP = {
    "carbohydrate": "carbohydrates(incl.fiber)",
    "fatty acid": "lipids",
    "amino acid": "amino acids and proteins",
    "nucleotide": "others",
}

VALID_SOURCES = {"fdc", "foodatlas", "dmd"}
VALID_SORT_COLS = {
    "common_name": "chemical_name",
    "median_concentration": "(median_concentration->>'value')::NUMERIC",
}
VALID_DIRECTIONS = {"ASC", "DESC"}

ALL_EVIDENCE_COLS = "fdc_evidences, foodatlas_evidences, dmd_evidences"
BASE_SELECT = (
    "chemical_name AS name, chemical_foodatlas_id AS id, "
    "chemical_classification, median_concentration"
)


async def get_metadata(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Get food entity metadata."""
    result = await session.execute(
        text("""
            SELECT common_name, foodatlas_id AS id, entity_type,
                   scientific_name, synonyms, external_ids, food_classification,
                   ambiguity_siblings
            FROM mv_food_entities WHERE common_name = :name
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    for row in data:
        row["external_ids"] = format_external_ids(row.get("external_ids"))
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_profile(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Get macro/micronutrient profile grouped by classification."""
    result = await session.execute(
        text("""
            SELECT chemical_name AS name, chemical_foodatlas_id AS id,
                   chemical_classification, median_concentration
            FROM mv_food_chemical_composition
            WHERE food_name = :name
            ORDER BY (median_concentration->>'value')::NUMERIC DESC NULLS LAST
        """),
        {"name": common_name},
    )
    profile: dict[str, list] = {
        "carbohydrates(incl.fiber)": [],
        "lipids": [],
        "vitamins": [],
        "amino acids and proteins": [],
        "minerals(incl.derivatives)": [],
        "others": [],
    }
    for row in result:
        mapping = row._mapping
        classifications = mapping["chemical_classification"] or []
        entry = {
            "id": mapping["id"],
            "name": mapping["name"],
            "median_concentration": mapping["median_concentration"],
        }
        for cls in classifications:
            key = NUTRIENT_KEY_MAP.get(cls)
            if key and key in profile:
                profile[key].append(entry)

    return {"data": profile}


async def get_composition_counts(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Get per-classification and per-source chemical counts."""
    result = await session.execute(
        text("""
            SELECT
                chemical_classification,
                CASE WHEN fdc_evidences IS NOT NULL THEN 1 ELSE 0 END AS has_fdc,
                CASE WHEN foodatlas_evidences IS NOT NULL THEN 1 ELSE 0 END AS has_fa,
                CASE WHEN dmd_evidences IS NOT NULL THEN 1 ELSE 0 END AS has_dmd
            FROM mv_food_chemical_composition
            WHERE food_name = :name
        """),
        {"name": common_name},
    )
    cls_counts: dict[str, int] = {}
    source_counts = {"fdc": 0, "foodatlas": 0, "dmd": 0}
    for row in result:
        mapping = row._mapping
        if mapping["has_fdc"]:
            source_counts["fdc"] += 1
        if mapping["has_fa"]:
            source_counts["foodatlas"] += 1
        if mapping["has_dmd"]:
            source_counts["dmd"] += 1
        classifications = mapping["chemical_classification"] or []
        if not classifications:
            cls_counts["n/a"] = cls_counts.get("n/a", 0) + 1
        else:
            for cls in classifications:
                cls_counts[cls] = cls_counts.get(cls, 0) + 1
    return {
        "data": {
            "classification_counts": cls_counts,
            "source_counts": source_counts,
        }
    }


async def get_composition(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    filter_source: str = "",
    search_term: str = "",
    sort_by: str = "common_name",
    sort_dir: str = "desc",
    show_all_rows: bool = True,
    filter_classification: str = "",
    rows_per_page: int = ROWS_PER_PAGE,
    trust: TrustMode = "default",
) -> dict[str, object]:
    """Get paginated food chemical composition with filtering/sorting.

    The ``trust`` param controls per-attestation visibility:
    ``default`` hides extractions whose llm_plausibility score is below
    :attr:`APISettings.trust_low_threshold`; ``show_all`` returns everything
    so the UI can render low-trust items with a warning icon; ``low_only``
    returns only the low-trust items. Filtering happens after pagination
    today, so page sizes for the non-default modes may be slightly under
    ``rows_per_page`` — acceptable trade-off for v1; revisit if pagination
    accuracy becomes a real UX issue.
    """
    sources = [s for s in filter_source.split("+") if s] if filter_source else []
    if filter_source and not sources:
        return _empty_composition(rows_per_page)

    classifications = (
        [c for c in filter_classification.split("+") if c]
        if filter_classification
        else []
    )

    # Validate and build query parts from allowlists (not user input)
    select_cols, where_parts, params = _build_query_parts(
        common_name,
        sources,
        search_term,
        show_all_rows,
        classifications,
    )
    sort_col = VALID_SORT_COLS.get(sort_by, "chemical_name")
    direction = sort_dir.upper() if sort_dir.upper() in VALID_DIRECTIONS else "DESC"

    where = " AND ".join(where_parts)
    offset = rows_per_page * (page - 1)

    # Fetch every matching row (no LIMIT/OFFSET in SQL). The trust filter
    # rewrites medians and drops rows, so SQL-level pagination would be
    # wrong for trust != show_all (rows can move pages once their median
    # changes). Total rows in tomato-scale foods is in the hundreds; the
    # cost of fetching all in one query is well below the cost of an
    # incorrect page count or the wrong rows on the page.
    sql = _compose_sql(select_cols, where, sort_col, direction, paginated=False)
    result = await session.execute(text(sql), params)
    all_rows = [dict(row._mapping) for row in result]

    threshold = APISettings().trust_low_threshold
    all_rows = await apply_trust_filter(
        session, all_rows, mode=trust, threshold=threshold
    )
    # Re-sort using the recomputed median (default / low_only) — show_all
    # keeps the stored median so SQL order is already correct.
    if trust != "show_all":
        all_rows = _resort_after_filter(all_rows, sort_by, direction)

    total_rows = len(all_rows)
    total_pages = (total_rows + rows_per_page - 1) // rows_per_page if total_rows else 0
    data = all_rows[offset : offset + rows_per_page]

    return {
        "data": data,
        "metadata": {
            "row_count": len(data),
            "rows_per_page": rows_per_page,
            "current_row": offset + 1,
            "current_page": page,
            "total_rows": total_rows,
            "total_pages": total_pages,
        },
    }


def _build_query_parts(
    common_name: str,
    sources: list[str],
    search_term: str,
    show_all_rows: bool,
    classifications: list[str] | None = None,
) -> tuple[str, list[str], dict]:
    """Build SELECT columns, WHERE conditions, and params from validated inputs."""
    # Evidence columns from allowlist
    valid = [s for s in sources if s in VALID_SOURCES]
    if not valid or len(valid) > 1:
        select_cols = BASE_SELECT + ", " + ALL_EVIDENCE_COLS
    else:
        select_cols = BASE_SELECT + ", " + valid[0] + "_evidences"

    conditions = ["food_name = :name"]
    params: dict = {"name": common_name}

    if len(valid) == 1:
        conditions.append(valid[0] + "_evidences IS NOT NULL")

    if search_term:
        if search_term.startswith("e") and search_term[1:].isdigit():
            conditions.append("chemical_foodatlas_id = :search")
            params["search"] = search_term
        else:
            conditions.append("chemical_name ILIKE :search")
            params["search"] = "%" + search_term + "%"

    if not show_all_rows:
        conditions.append("median_concentration IS NOT NULL")

    if classifications:
        has_named = [c for c in classifications if c != "n/a"]
        has_unclassified = "n/a" in classifications
        cls_parts: list[str] = []
        for i, cls in enumerate(has_named):
            key = f"cls_{i}"
            cls_parts.append(f":{key} = ANY(chemical_classification)")
            params[key] = cls
        if has_unclassified:
            cls_parts.append("chemical_classification = '{}'")
        conditions.append("(" + " OR ".join(cls_parts) + ")")

    return select_cols, conditions, params


def _compose_sql(
    select_cols: str,
    where: str,
    sort_col: str,
    direction: str,
    *,
    paginated: bool = False,
    count_only: bool = False,
) -> str:
    """Compose SQL from pre-validated parts."""
    if count_only:
        return "SELECT COUNT(*) FROM mv_food_chemical_composition WHERE " + where
    pagination = ""
    if paginated:
        pagination = " OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY"
    return (
        "SELECT "
        + select_cols
        + " FROM mv_food_chemical_composition WHERE "
        + where
        + " ORDER BY "
        + sort_col
        + " "
        + direction
        + " NULLS LAST"
        + pagination
    )


def _resort_after_filter(data: list[dict], sort_by: str, direction: str) -> list[dict]:
    """Re-sort the page using the post-filter (recomputed) median.

    Pagination is unchanged — we operate on whatever rows the SQL page
    returned. This restores within-page ordering after the trust filter
    rewrites medians; cross-page ordering can still be approximate.
    """
    descending = direction.upper() == "DESC"
    if sort_by == "median_concentration":
        with_val: list[dict] = []
        without_val: list[dict] = []
        for row in data:
            mc = row.get("median_concentration")
            val = mc.get("value") if isinstance(mc, dict) else None
            (with_val if val is not None else without_val).append(row)
        with_val.sort(
            key=lambda r: r["median_concentration"]["value"], reverse=descending
        )
        return with_val + without_val  # NULLS LAST
    if sort_by == "common_name":
        return sorted(
            data, key=lambda r: (r.get("name") or "").lower(), reverse=descending
        )
    return data


def _empty_composition(rows_per_page: int) -> dict[str, object]:
    return {
        "data": [],
        "metadata": {
            "row_count": 0,
            "rows_per_page": rows_per_page,
            "current_row": 0,
            "current_page": 0,
            "total_rows": 0,
            "total_pages": 0,
        },
    }
