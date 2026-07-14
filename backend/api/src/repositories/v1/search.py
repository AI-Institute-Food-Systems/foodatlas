"""Search + stats for /v1/.

Reuses the existing ``mv_search_auto_complete`` and ``mv_metadata_statistics``
materialised views, but returns a flat shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from .pagination import offset as _offset

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def search(
    session: AsyncSession,
    *,
    q: str,
    entity_type: str = "",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """Trigram autocomplete; optional ``entity_type`` filter."""
    word = q.lower().strip()
    if not word:
        return [], 0

    where = ["substr_auto LIKE :pattern"]
    params: dict[str, object] = {
        "pattern": f"%{word}%",
        "prefix": f"{word}%",
        "word": word,
    }
    if entity_type:
        where.append("entity_type = :etype")
        params["etype"] = entity_type
    where_sql = " AND ".join(where)

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_search_auto_complete WHERE {where_sql}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    # Bucketed ranking: exact token → prefix token → substring. See
    # ``repositories/search.py`` for the rationale — kept identical here so
    # /search and /v1/search rank results the same way.
    sql = f"""
        SELECT
            foodatlas_id AS id,
            entity_type,
            common_name,
            scientific_name,
            associations
        FROM mv_search_auto_complete
        WHERE {where_sql}
        ORDER BY
            CASE
                WHEN exact_auto @> ARRAY[:word] THEN 1
                WHEN EXISTS (
                    SELECT 1 FROM unnest(exact_auto) AS t WHERE t LIKE :prefix
                ) THEN 2
                ELSE 3
            END,
            associations DESC,
            similarity(substr_auto, :word) DESC
        OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
    """
    result = await session.execute(text(sql), params)
    return [dict(r._mapping) for r in result], total


_STAT_KEY_MAP = {
    "number of foods": "foods",
    "number of chemicals": "chemicals",
    "number of diseases": "diseases",
    "number of bioactivities": "bioactivities",
    "number of bioactivity measurements": "bioactivity_measurements",
    "number of publications": "publications",
    "number of associations": "connections",
}


async def get_stats(session: AsyncSession) -> dict[str, int]:
    result = await session.execute(
        text("SELECT field, count FROM mv_metadata_statistics")
    )
    out: dict[str, int] = dict.fromkeys(_STAT_KEY_MAP.values(), 0)
    for row in result:
        mapping = row._mapping
        key = _STAT_KEY_MAP.get(mapping["field"])
        if key:
            out[key] = int(mapping["count"])
    return out
