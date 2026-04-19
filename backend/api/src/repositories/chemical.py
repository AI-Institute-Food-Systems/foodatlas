"""Chemical entity repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .formatting import format_external_ids

ROWS_PER_PAGE = 10


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
    with_conc = await session.execute(
        text("""
            WITH chem_foods AS (
                SELECT food_foodatlas_id AS id
                FROM mv_food_chemical_composition
                WHERE chemical_name = :name
            )
            SELECT c.food_foodatlas_id AS id, c.food_name AS name,
                   c.median_concentration,
                   COALESCE((
                       SELECT jsonb_agg(s ORDER BY s->>'common_name')
                       FROM jsonb_array_elements(fe.ambiguity_siblings) s
                       WHERE s->>'foodatlas_id' IN (SELECT id FROM chem_foods)
                   ), '[]'::jsonb) AS ambiguity_siblings
            FROM mv_food_chemical_composition c
            LEFT JOIN mv_food_entities fe
                ON fe.foodatlas_id = c.food_foodatlas_id
            WHERE c.chemical_name = :name AND c.median_concentration IS NOT NULL
        """),
        {"name": common_name},
    )
    without_conc = await session.execute(
        text("""
            WITH chem_foods AS (
                SELECT food_foodatlas_id AS id
                FROM mv_food_chemical_composition
                WHERE chemical_name = :name
            )
            SELECT c.food_foodatlas_id AS id, c.food_name AS name,
                   COALESCE(jsonb_array_length(c.fdc_evidences), 0)
                   + COALESCE(jsonb_array_length(c.foodatlas_evidences), 0)
                   + COALESCE(jsonb_array_length(c.dmd_evidences), 0)
                   AS evidence_count,
                   COALESCE((
                       SELECT jsonb_agg(s ORDER BY s->>'common_name')
                       FROM jsonb_array_elements(fe.ambiguity_siblings) s
                       WHERE s->>'foodatlas_id' IN (SELECT id FROM chem_foods)
                   ), '[]'::jsonb) AS ambiguity_siblings
            FROM mv_food_chemical_composition c
            LEFT JOIN mv_food_entities fe
                ON fe.foodatlas_id = c.food_foodatlas_id
            WHERE c.chemical_name = :name AND c.median_concentration IS NULL
            ORDER BY COALESCE(jsonb_array_length(c.fdc_evidences), 0)
                   + COALESCE(jsonb_array_length(c.foodatlas_evidences), 0)
                   + COALESCE(jsonb_array_length(c.dmd_evidences), 0) DESC
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
    relation: str = "positive",
    rows_per_page: int = ROWS_PER_PAGE,
) -> dict[str, object]:
    """Get disease correlations for a chemical.

    relation="positive" -> r4 (helps reduce disease)
    relation="negative" -> r3 (worsens disease)
    """
    relationship_id = "r4" if relation == "positive" else "r3"
    offset = rows_per_page * (page - 1)

    result = await session.execute(
        text("""
            SELECT disease_foodatlas_id AS id, disease_name AS name,
                   source_chemical_name, source_chemical_foodatlas_id,
                   sources, evidences, evidence_count
            FROM mv_chemical_disease_correlation
            WHERE chemical_name = :name AND relationship_id = :rel
            ORDER BY evidence_count DESC
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {
            "name": common_name,
            "rel": relationship_id,
            "offset": offset,
            "limit": rows_per_page,
        },
    )
    data = [dict(r._mapping) for r in result]

    count_result = await session.execute(
        text("""
            SELECT COUNT(*) FROM mv_chemical_disease_correlation
            WHERE chemical_name = :name AND relationship_id = :rel
        """),
        {"name": common_name, "rel": relationship_id},
    )
    total_rows = count_result.scalar() or 0
    total_pages = (total_rows + rows_per_page - 1) // rows_per_page if total_rows else 0

    positive = data if relation == "positive" else None
    negative = data if relation == "negative" else None

    return {
        "data": {
            "positive_associations": positive,
            "negative_associations": negative,
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
