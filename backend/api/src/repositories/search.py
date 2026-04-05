"""Search and statistics repository."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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

    # Main query: substring match with exact boost + similarity ranking
    query = text("""
        SELECT foodatlas_id, associations, entity_type, common_name,
               scientific_name, synonyms, external_ids
        FROM mv_search_auto_complete
        WHERE substr_auto LIKE :pattern
        ORDER BY
            CASE WHEN exact_auto @> ARRAY[:word] THEN 1 ELSE 2 END,
            associations DESC,
            similarity(substr_auto, :word) DESC
        OFFSET :offset ROWS
        FETCH FIRST :limit ROWS ONLY
    """)
    params = {
        "pattern": f"%{word}%",
        "word": word,
        "offset": offset,
        "limit": rows_per_page,
    }
    result = await session.execute(query, params)
    data = [dict(row._mapping) for row in result]

    # Count query
    count_query = text("""
        SELECT COUNT(*) FROM mv_search_auto_complete
        WHERE substr_auto LIKE :pattern
    """)
    count_result = await session.execute(count_query, {"pattern": f"%{word}%"})
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
