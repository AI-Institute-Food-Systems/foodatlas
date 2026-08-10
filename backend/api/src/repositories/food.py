"""Food entity repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import APISettings

from ._search_util import build_ilike_pattern
from .formatting import format_external_ids
from .trust_filter import (
    TrustMode,
    _fetch_trust_scores,
    apply_trust_filter,
)

ROWS_PER_PAGE = 25

NUTRIENT_KEY_MAP = {
    "carbohydrate": "carbohydrates(incl.fiber)",
    "fatty acid": "lipids",
    "amino acid": "amino acids and proteins",
    "nucleotide": "others",
}

# DMD (Dairy Molecule Database) is retired from the public API surface
# 2026-07-06 — the DB column `dmd_evidences` stays populated on
# `mv_food_chemical_composition` but is no longer selectable / filterable
# / countable via this API.
# PTFI ships relative_abundance rather than mg/100g, so most of its rows
# have no median_concentration — they are still real composition evidence
# and are selectable/filterable/countable like any other source.
VALID_SOURCES = {"fdc", "foodatlas", "ptfi"}
VALID_SORT_COLS = {
    "common_name": "chemical_name",
    "median_concentration": "(median_concentration->>'value')::NUMERIC",
    "evidence_count": (
        "COALESCE(jsonb_array_length(fdc_evidences), 0) "
        "+ COALESCE(jsonb_array_length(foodatlas_evidences), 0) "
        "+ COALESCE(jsonb_array_length(ptfi_evidences), 0) "
        "+ COALESCE(jsonb_array_length(dmd_evidences), 0)"
    ),
}
VALID_DIRECTIONS = {"ASC", "DESC"}

ALL_EVIDENCE_COLS = "fdc_evidences, foodatlas_evidences, ptfi_evidences"
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


def _collect_attestation_ids(row: dict) -> list[str]:
    """All attestation_ids across a composition row's evidence lists."""
    atts: list[str] = []
    for ev_list in (
        row["fdc_evidences"],
        row["foodatlas_evidences"],
        row.get("ptfi_evidences"),
    ):
        for ev in ev_list or []:
            for ext in ev.get("extraction") or []:
                aid = ext.get("attestation_id")
                if aid:
                    atts.append(aid)
    return atts


def _annotate_composition_rows(
    rows: list[dict], scores: dict[str, float], threshold: float
) -> None:
    """Precompute per-row facets used by the counts loops."""
    for r in rows:
        atts = _collect_attestation_ids(r)
        r["_classifications"] = r["chemical_classification"] or []
        r["_has_fdc"] = r["fdc_evidences"] is not None
        r["_has_fa"] = r["foodatlas_evidences"] is not None
        r["_has_ptfi"] = r.get("ptfi_evidences") is not None
        r["_has_conc"] = r["median_concentration"] is not None
        r["_fully_low"] = bool(atts) and all(
            aid in scores and scores[aid] <= threshold for aid in atts
        )
        r["_name_lower"] = (r["chemical_name"] or "").lower()


async def get_composition_counts(
    session: AsyncSession,
    common_name: str,
    filter_source: str = "",
    filter_classification: str = "",
    show_all_rows: bool = True,
    trust: TrustMode = "default",
    search_term: str = "",
) -> dict[str, object]:
    """Faceted composition counts.

    Each per-dimension count applies every *other* filter currently
    active and reports how many rows the dimension governs — so as the
    user narrows the view, each count answers "if I toggled this option
    now, how many rows would change?".

    - ``classification_counts`` — apply source + concentration + trust +
      search; group by classification.
    - ``source_counts`` — apply class + concentration + trust + search;
      count rows per source (fdc / foodatlas).
    - ``no_concentration_count`` — apply source + class + trust + search;
      count rows whose median_concentration is NULL. This is the number
      of rows the "Include without concentration" toggle governs.
    - ``low_trust_count`` — apply source + class + concentration +
      search; count rows the low-trust filter would drop. This is the
      number of rows the "Include low-trust data points" toggle governs.
      trust_filter._is_low treats unscored attestations (FDC, un-judged
      lit2kg) as high-trust, so a row is only dropped when *every*
      extraction is both scored AND at or below the threshold.
    """
    result = await session.execute(
        text("""
            SELECT id, chemical_name, chemical_classification,
                   median_concentration, fdc_evidences, foodatlas_evidences,
                   ptfi_evidences
            FROM mv_food_chemical_composition
            WHERE food_name = :name
              AND (fdc_evidences IS NOT NULL
                   OR foodatlas_evidences IS NOT NULL
                   OR ptfi_evidences IS NOT NULL)
        """),
        {"name": common_name},
    )
    rows = [dict(r._mapping) for r in result]

    # One round-trip for all trust scores across all rows' extractions,
    # then annotate each row with the facet flags the counts loops use.
    all_att_ids: set[str] = {aid for r in rows for aid in _collect_attestation_ids(r)}
    scores = (
        await _fetch_trust_scores(session, list(all_att_ids)) if all_att_ids else {}
    )
    threshold = APISettings().trust_low_threshold
    _annotate_composition_rows(rows, scores, threshold)

    active_sources = (
        {s for s in filter_source.split("+") if s}
        if filter_source
        else {"fdc", "foodatlas"}
    )
    active_classes = (
        {c for c in filter_classification.split("+") if c}
        if filter_classification
        else set()
    )
    q = search_term.strip().lower()
    trust_default = trust == "default"

    def m_source(r: dict) -> bool:
        return (
            ("fdc" in active_sources and r["_has_fdc"])
            or ("foodatlas" in active_sources and r["_has_fa"])
            or ("ptfi" in active_sources and r["_has_ptfi"])
        )

    def m_class(r: dict) -> bool:
        if not active_classes:
            return True
        if "n/a" in active_classes and not r["_classifications"]:
            return True
        return any(cls in active_classes for cls in r["_classifications"])

    def m_conc(r: dict) -> bool:
        return show_all_rows or r["_has_conc"]

    def m_trust(r: dict) -> bool:
        return not trust_default or not r["_fully_low"]

    def m_search(r: dict) -> bool:
        return not q or q in r["_name_lower"]

    # classification_counts — exclude class filter.
    cls_counts: dict[str, int] = {}
    for r in rows:
        if not (m_source(r) and m_conc(r) and m_trust(r) and m_search(r)):
            continue
        classifications = r["_classifications"]
        if not classifications:
            cls_counts["n/a"] = cls_counts.get("n/a", 0) + 1
        else:
            for cls in classifications:
                cls_counts[cls] = cls_counts.get(cls, 0) + 1

    # source_counts — exclude source filter.
    source_counts = {"fdc": 0, "foodatlas": 0, "ptfi": 0}
    for r in rows:
        if not (m_class(r) and m_conc(r) and m_trust(r) and m_search(r)):
            continue
        if r["_has_fdc"]:
            source_counts["fdc"] += 1
        if r["_has_fa"]:
            source_counts["foodatlas"] += 1
        if r["_has_ptfi"]:
            source_counts["ptfi"] += 1

    # Toggle counts — exclude the toggle's own filter, count rows the
    # toggle governs.
    no_concentration_count = sum(
        1
        for r in rows
        if m_source(r)
        and m_class(r)
        and m_trust(r)
        and m_search(r)
        and not r["_has_conc"]
    )
    low_trust_count = sum(
        1
        for r in rows
        if m_source(r) and m_class(r) and m_conc(r) and m_search(r) and r["_fully_low"]
    )

    return {
        "data": {
            "classification_counts": cls_counts,
            "source_counts": source_counts,
            "no_concentration_count": no_concentration_count,
            "low_trust_count": low_trust_count,
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
    find_chemical: str = "",
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

    # If find_chemical is given, locate the row in the unfiltered sorted list
    # and serve the page containing it. This overrides the requested page so
    # the client can land directly on the right page without a second round
    # trip. Match is case-insensitive against chemical_name; foodatlas_id is
    # also accepted (passes through unchanged).
    highlight_page: int | None = None
    if find_chemical:
        needle = find_chemical.lower()
        for i, row in enumerate(all_rows):
            name = str(row.get("chemical_name") or row.get("name") or "").lower()
            fid = str(row.get("chemical_foodatlas_id") or row.get("id") or "").lower()
            if needle in (name, fid):
                highlight_page = i // rows_per_page + 1
                break
        if highlight_page is not None:
            page = highlight_page
            offset = rows_per_page * (page - 1)

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
            "highlight_page": highlight_page,
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

    conditions = [
        "food_name = :name",
        # Filter DMD-only rows unconditionally — the public API stopped
        # exposing dmd_evidences in the 2026-07-06 DMD removal (PR #249)
        # so a row with only DMD evidence renders as an empty row on the
        # composition table. Every source we DO expose must be listed
        # here, otherwise its rows are silently dropped: PTFI rows carry
        # only ptfi_evidences, so omitting it would hide all 11k of them.
        "(fdc_evidences IS NOT NULL"
        " OR foodatlas_evidences IS NOT NULL"
        " OR ptfi_evidences IS NOT NULL)",
    ]
    params: dict = {"name": common_name}

    if len(valid) == 1:
        conditions.append(valid[0] + "_evidences IS NOT NULL")

    if search_term:
        cleaned = search_term.strip()
        if cleaned.startswith("e") and cleaned[1:].isdigit():
            conditions.append("chemical_foodatlas_id = :search")
            params["search"] = cleaned
        else:
            search_pattern = build_ilike_pattern(cleaned)
            if search_pattern:
                conditions.append("chemical_name ILIKE :search")
                params["search"] = search_pattern

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
    if sort_by == "evidence_count":
        return sorted(
            data,
            key=lambda r: (
                len(r.get("fdc_evidences") or [])
                + len(r.get("foodatlas_evidences") or [])
                + len(r.get("dmd_evidences") or [])
            ),
            reverse=descending,
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
