"""Disease entity repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .formatting import format_external_ids

ROWS_PER_PAGE = 10


async def get_metadata(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Get disease entity metadata."""
    result = await session.execute(
        text("""
            SELECT common_name, foodatlas_id AS id, entity_type,
                   scientific_name, synonyms, external_ids,
                   ambiguity_siblings
            FROM mv_disease_entities WHERE common_name = :name
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    for row in data:
        row["external_ids"] = format_external_ids(row.get("external_ids"))
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_correlation(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    relation: str = "positive",
    rows_per_page: int = ROWS_PER_PAGE,
) -> dict[str, object]:
    """Get chemical correlations for a disease.

    relation="positive" -> r4 (helps reduce disease)
    relation="negative" -> r3 (worsens disease)
    """
    relationship_id = "r4" if relation == "positive" else "r3"
    offset = rows_per_page * (page - 1)

    result = await session.execute(
        text("""
            WITH disease_chems AS (
                SELECT chemical_foodatlas_id AS id
                FROM mv_chemical_disease_correlation
                WHERE disease_name = :name AND relationship_id = :rel
            )
            SELECT c.chemical_foodatlas_id AS id, c.chemical_name AS name,
                   c.sources, c.evidences, c.evidence_count,
                   COALESCE((
                       SELECT jsonb_agg(s ORDER BY s->>'common_name')
                       FROM jsonb_array_elements(ce.ambiguity_siblings) s
                       WHERE s->>'foodatlas_id' IN (SELECT id FROM disease_chems)
                   ), '[]'::jsonb) AS ambiguity_siblings
            FROM mv_chemical_disease_correlation c
            LEFT JOIN mv_chemical_entities ce
                ON ce.foodatlas_id = c.chemical_foodatlas_id
            WHERE c.disease_name = :name AND c.relationship_id = :rel
            ORDER BY c.evidence_count DESC
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
            WHERE disease_name = :name AND relationship_id = :rel
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
