"""Chemical entity repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from . import _correlation
from .formatting import format_external_ids

# 25 to match food.py. The merged Diseases tab stacks two tables, so a
# 10-row page turned the CTD half into mostly pagination chrome.
ROWS_PER_PAGE = 25

# Static SQL fragments (no user input — interpolated, never parameterised),
# following the BASE_SELECT / ALL_EVIDENCE_COLS precedent in food.py. Both
# composition buckets share them so their evidence arithmetic cannot drift.
#
# DMD is excluded on purpose: `dmd_evidences` stays populated in the view but
# left the public API in the 2026-07-06 removal, so counting it here would
# report data points the UI can never show.
_SOURCE_COUNTS = (
    "COALESCE(jsonb_array_length(c.fdc_evidences), 0) AS fdc_count, "
    "COALESCE(jsonb_array_length(c.foodatlas_evidences), 0) AS foodatlas_count, "
    "COALESCE(jsonb_array_length(c.ptfi_evidences), 0) AS ptfi_count"
)
_EVIDENCE_COUNT = (
    "COALESCE(jsonb_array_length(c.fdc_evidences), 0) "
    "+ COALESCE(jsonb_array_length(c.foodatlas_evidences), 0) "
    "+ COALESCE(jsonb_array_length(c.ptfi_evidences), 0)"
)


async def get_metadata(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Get chemical entity metadata."""
    result = await session.execute(
        text("""
            SELECT common_name, foodatlas_id AS id, entity_type,
                   scientific_name, synonyms, external_ids,
                   chemical_classification, flavor_descriptors,
                   ambiguity_siblings
            FROM mv_chemical_entities WHERE common_name = :name
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    for row in data:
        row["external_ids"] = format_external_ids(row.get("external_ids"))
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_composition(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Get foods containing this chemical, split by concentration.

    Each row's ``ambiguity_siblings`` is scoped to foods that also appear in
    this chemical's composition. Foods that share a name-cluster globally
    but have no attestation for this chemical are filtered out so the banner
    stays consistent with what the page actually shows.
    """
    # Filter DMD-only rows unconditionally — the public API stopped
    # exposing dmd_evidences in the 2026-07-06 DMD removal (PR #249),
    # so a row with only DMD evidence would appear here with no visible
    # evidence. The `chem_foods` CTE gets the same filter so ambiguity
    # siblings stay consistent with what actually shows in the table.
    with_conc = await session.execute(
        text(f"""
            WITH chem_foods AS (
                SELECT food_foodatlas_id AS id
                FROM mv_food_chemical_composition
                WHERE chemical_name = :name
                  AND (fdc_evidences IS NOT NULL
                       OR foodatlas_evidences IS NOT NULL
                       OR ptfi_evidences IS NOT NULL)
            )
            SELECT c.food_foodatlas_id AS id, c.food_name AS name,
                   c.median_concentration,
                   {_SOURCE_COUNTS},
                   {_EVIDENCE_COUNT} AS evidence_count,
                   COALESCE((
                       SELECT jsonb_agg(s ORDER BY s->>'common_name')
                       FROM jsonb_array_elements(fe.ambiguity_siblings) s
                       WHERE s->>'foodatlas_id' IN (SELECT id FROM chem_foods)
                   ), '[]'::jsonb) AS ambiguity_siblings
            FROM mv_food_chemical_composition c
            LEFT JOIN mv_food_entities fe
                ON fe.foodatlas_id = c.food_foodatlas_id
            WHERE c.chemical_name = :name
              AND c.median_concentration IS NOT NULL
              AND (c.fdc_evidences IS NOT NULL
                   OR c.foodatlas_evidences IS NOT NULL
                   OR c.ptfi_evidences IS NOT NULL)
            ORDER BY (c.median_concentration->>'value')::NUMERIC DESC NULLS LAST
        """),
        {"name": common_name},
    )
    without_conc = await session.execute(
        text(f"""
            WITH chem_foods AS (
                SELECT food_foodatlas_id AS id
                FROM mv_food_chemical_composition
                WHERE chemical_name = :name
                  AND (fdc_evidences IS NOT NULL
                       OR foodatlas_evidences IS NOT NULL
                       OR ptfi_evidences IS NOT NULL)
            )
            SELECT c.food_foodatlas_id AS id, c.food_name AS name,
                   {_SOURCE_COUNTS},
                   {_EVIDENCE_COUNT} AS evidence_count,
                   COALESCE((
                       SELECT jsonb_agg(s ORDER BY s->>'common_name')
                       FROM jsonb_array_elements(fe.ambiguity_siblings) s
                       WHERE s->>'foodatlas_id' IN (SELECT id FROM chem_foods)
                   ), '[]'::jsonb) AS ambiguity_siblings
            FROM mv_food_chemical_composition c
            LEFT JOIN mv_food_entities fe
                ON fe.foodatlas_id = c.food_foodatlas_id
            WHERE c.chemical_name = :name
              AND c.median_concentration IS NULL
              AND (c.fdc_evidences IS NOT NULL
                   OR c.foodatlas_evidences IS NOT NULL
                   OR c.ptfi_evidences IS NOT NULL)
            ORDER BY {_EVIDENCE_COUNT} DESC
        """),
        {"name": common_name},
    )
    with_data = [dict(r._mapping) for r in with_conc]
    without_data = [dict(r._mapping) for r in without_conc]

    return {
        "data": {
            "with_concentrations": with_data,
            "without_concentrations": without_data,
        },
        "metadata": {
            "row_count": len(with_data) + len(without_data),
        },
    }


async def get_correlation(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    relation: str = "all",
    search: str = "",
    rows_per_page: int = ROWS_PER_PAGE,
) -> dict[str, object]:
    """Get disease correlations for a chemical.

    relation="positive" -> r4 (helps reduce disease)
    relation="negative" -> r3 (worsens disease)
    relation="all"      -> both, with the direction carried per row

    "all" is the default because the merged Diseases tab renders one
    table with a direction column rather than a table per direction. The
    two named directions stay for the sidebar's Direction facet.

    ``relationship_id`` is selected in every mode so a row can render its
    own direction badge without the caller having to remember which
    query produced it.
    """
    where, filter_params = _correlation.build_filters(relation, search, "disease_name")
    offset = rows_per_page * (page - 1)

    result = await session.execute(
        text(f"""
            SELECT disease_foodatlas_id AS id, disease_name AS name,
                   relationship_id,
                   source_chemical_name, source_chemical_foodatlas_id,
                   sources, evidences, evidence_count
            FROM {_correlation.VIEW}
            WHERE chemical_name = :name{where}
            ORDER BY evidence_count DESC, disease_name
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {
            "name": common_name,
            "offset": offset,
            "limit": rows_per_page,
            **filter_params,
        },
    )
    data = [dict(r._mapping) for r in result]

    count_result = await session.execute(
        text(f"""
            SELECT COUNT(*) FROM {_correlation.VIEW}
            WHERE chemical_name = :name{where}
        """),
        {"name": common_name, **filter_params},
    )
    total_rows = count_result.scalar() or 0
    total_pages = (total_rows + rows_per_page - 1) // rows_per_page if total_rows else 0

    return {
        "data": {
            # Canonical key: the page as returned, whatever the direction
            # filter was. The two below are kept so the older
            # one-table-per-direction callers keep working.
            "associations": data,
            "positive_associations": data if relation == "positive" else None,
            "negative_associations": data if relation == "negative" else None,
        },
        "metadata": {
            "row_count": len(data),
            "rows_per_page": rows_per_page,
            "current_row": offset + 1,
            "current_page": page,
            "total_rows": total_rows,
            "total_pages": total_pages,
        },
    }


async def get_correlation_direction_counts(
    session: AsyncSession, common_name: str, search: str = ""
) -> dict[str, int]:
    """Improves/worsens counts for this chemical, under the active search."""
    return await _correlation.get_direction_counts(
        session, "chemical_name", "disease_name", common_name, search
    )


async def get_composition_evidence(
    session: AsyncSession, common_name: str, food_name: str
) -> dict[str, object]:
    """Evidence records behind one (chemical, food) composition row.

    Fetched on demand rather than inlined into ``get_composition``. Quercetin
    alone carries 6.7 MB of evidence JSON across its 464 foods, against a
    93 KB composition payload — and that payload is fetched server-side on
    every chemical page load, for a modal most visitors never open. A single
    pair averages ~15 KB.

    DMD is excluded here for the same reason it is excluded from the counts:
    it left the public API in the 2026-07-06 removal, so returning it would
    show data points the counts never promised.
    """
    result = await session.execute(
        text("""
            SELECT fdc_evidences, foodatlas_evidences, ptfi_evidences
            FROM mv_food_chemical_composition
            WHERE chemical_name = :chemical AND food_name = :food
            LIMIT 1
        """),
        {"chemical": common_name, "food": food_name},
    )
    row = result.mappings().first()
    if row is None:
        return {"data": [], "metadata": {"row_count": 0}}

    # Flattened, because the modal takes one list and sorts it itself. The
    # source of each record is already on the record.
    evidences = [
        ev
        for key in ("fdc_evidences", "foodatlas_evidences", "ptfi_evidences")
        for ev in (row[key] or [])
    ]
    return {"data": evidences, "metadata": {"row_count": len(evidences)}}
