"""Disease entity repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from . import _correlation
from .formatting import format_external_ids

# 25 to match food.py — see the note on chemical.ROWS_PER_PAGE.
ROWS_PER_PAGE = 25


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
    relation: str = "all",
    search: str = "",
    rows_per_page: int = ROWS_PER_PAGE,
) -> dict[str, object]:
    """Get chemical correlations for a disease.

    relation="positive" -> r4 (helps reduce disease)
    relation="negative" -> r3 (worsens disease)
    relation="all"      -> both, with the direction carried per row

    Mirror of :func:`chemical.get_correlation`; see it for why "all" is
    the default.
    """
    # The page query aliases the view as `c`, the count query does not,
    # so the peer column has to be qualified per call site.
    where_c, filter_params = _correlation.build_filters(
        relation, search, "c.chemical_name"
    )
    where, _ = _correlation.build_filters(relation, search, "chemical_name")
    offset = rows_per_page * (page - 1)

    result = await session.execute(
        text(f"""
            WITH disease_chems AS (
                SELECT chemical_foodatlas_id AS id
                FROM {_correlation.VIEW}
                WHERE disease_name = :name{where}
            )
            SELECT c.chemical_foodatlas_id AS id, c.chemical_name AS name,
                   {_correlation.PAIR_AGGREGATES},
                   -- Scalar subquery rather than a join + aggregate: this
                   -- depends only on chemical_foodatlas_id, which is a
                   -- grouping column, and jsonb has no MIN to collapse it
                   -- with once the rows are grouped.
                   COALESCE((
                       SELECT jsonb_agg(s ORDER BY s->>'common_name')
                       FROM mv_chemical_entities ce,
                            jsonb_array_elements(ce.ambiguity_siblings) s
                       WHERE ce.foodatlas_id = c.chemical_foodatlas_id
                         AND s->>'foodatlas_id' IN (SELECT id FROM disease_chems)
                   ), '[]'::jsonb) AS ambiguity_siblings
            FROM {_correlation.VIEW} c
            WHERE c.disease_name = :name{where_c}
            GROUP BY {_correlation.GROUP_BY_PAIR}
            ORDER BY SUM(c.evidence_count) DESC, c.chemical_name
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {
            "name": common_name,
            "offset": offset,
            "limit": rows_per_page,
            **filter_params,
        },
    )
    data = _correlation.shape_pair_rows([dict(r._mapping) for r in result])

    count_result = await session.execute(
        text(f"""
            SELECT COUNT(*) FROM (
                SELECT 1 FROM {_correlation.VIEW}
                WHERE disease_name = :name{where}
                GROUP BY {_correlation.GROUP_BY_PAIR}
            ) pairs
        """),
        {"name": common_name, **filter_params},
    )
    total_rows = count_result.scalar() or 0
    total_pages = (total_rows + rows_per_page - 1) // rows_per_page if total_rows else 0

    return {
        "data": {
            # See chemical.get_correlation for why all three keys exist.
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
    """Improves/worsens counts for this disease, under the active search."""
    return await _correlation.get_direction_counts(
        session, "disease_name", "chemical_name", common_name, search
    )
