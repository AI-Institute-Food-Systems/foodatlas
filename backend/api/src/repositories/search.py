"""Search and statistics repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ._search_util import escape_like, foodatlas_id_pattern

ROWS_PER_PAGE = 10


async def search(
    session: AsyncSession,
    term: str,
    page: int = 1,
    rows_per_page: int = ROWS_PER_PAGE,
) -> dict[str, object]:
    """Search entities with autocomplete, pg_trgm ranking."""
    word = term.lower().strip()
    offset = rows_per_page * (page - 1)

    # `%` and `_` are LIKE metacharacters, and users type both: searching "%"
    # returned all 9,141 entities, and DMD peptide names like CBL_0001 contain
    # a literal underscore. `word` stays raw — it feeds array containment and
    # similarity(), neither of which is a LIKE pattern.
    escaped = escape_like(word)
    params: dict[str, object] = {
        "pattern": f"%{escaped}%",
        "prefix": f"{escaped}%",
        "word": word,
    }

    # `substr_auto` tokenizes names, synonyms and external IDs but not the
    # FoodAtlas ID, so pasting `e2908` matched nothing even though the results
    # UI highlights the ID as if it were searchable. Only added for ID-shaped
    # terms, so ordinary queries keep their existing plan and bind parameters.
    where_sql = "substr_auto LIKE :pattern"
    id_pattern = foodatlas_id_pattern(word)
    if id_pattern:
        where_sql = f"({where_sql} OR foodatlas_id LIKE :id_pattern)"
        params["id_pattern"] = id_pattern

    # Bucketed ranking: exact ID → exact token → prefix token → substring.
    # Prevents high-association but weakly-matched entities (e.g.
    # `hepatomegaly` for the query `tom`) from burying obvious prefix matches
    # like `tomato`, and keeps a fully-typed ID ahead of its prefix siblings
    # (`e2908` before `e29080`). The ID bucket is unconditional — `:word` is
    # always bound and never matches an ID for an ordinary term.
    # Ordering within each bucket keeps the legacy associations-first prior.
    query = text(f"""
        SELECT foodatlas_id, associations, entity_type, common_name,
               scientific_name, synonyms, external_ids
        FROM mv_search_auto_complete
        WHERE {where_sql}
        ORDER BY
            CASE
                WHEN foodatlas_id = :word THEN 0
                WHEN exact_auto @> ARRAY[:word] THEN 1
                WHEN EXISTS (
                    SELECT 1 FROM unnest(exact_auto) AS t WHERE t LIKE :prefix
                ) THEN 2
                ELSE 3
            END,
            associations DESC,
            similarity(substr_auto, :word) DESC
        OFFSET :offset ROWS
        FETCH FIRST :limit ROWS ONLY
    """)
    result = await session.execute(
        query, {**params, "offset": offset, "limit": rows_per_page}
    )
    data = [dict(row._mapping) for row in result]

    # Count query — same WHERE clause and parameters, or `total_rows` would
    # disagree with the rows actually returned.
    count_query = text(f"""
        SELECT COUNT(*) FROM mv_search_auto_complete
        WHERE {where_sql}
    """)
    count_result = await session.execute(count_query, params)
    total_rows = count_result.scalar() or 0
    total_pages = (total_rows + rows_per_page - 1) // rows_per_page if total_rows else 0

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


async def get_statistics(session: AsyncSession) -> dict[str, object]:
    """Get aggregate statistics for the landing page."""
    result = await session.execute(
        text("SELECT field, count FROM mv_metadata_statistics")
    )
    rows = result.fetchall()

    key_map = {
        "number of foods": "foods",
        "number of chemicals": "chemicals",
        "number of diseases": "diseases",
        "number of publications": "publications",
        "number of associations": "connections",
        "number of bioactivities": "bioactivities",
        "number of bioactivity measurements": "bioactivity_measurements",
    }
    statistics = {}
    for row in rows:
        mapped = key_map.get(row.field)
        if mapped:
            statistics[mapped] = row.count

    return {
        "data": {"statistics": statistics},
        "metadata": {"row_count": len(rows)},
    }
