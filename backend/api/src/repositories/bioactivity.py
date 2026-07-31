"""Bioactivity repository — queries the bioactivity materialized views.

Reads only ``mv_bioactivity_entities`` / ``mv_chemical_bioactivity`` /
``mv_food_bioactivity`` (never base tables), matching the rest of the API.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500  # cap so a huge ?limit can't re-inflate the response to a 502


def _page_envelope(
    data: list[dict], page: int, limit: int, total: int
) -> dict[str, object]:
    total_pages = (total + limit - 1) // limit if total else 0
    return {
        "data": data,
        "metadata": {
            "row_count": len(data),
            "page": page,
            "rows_per_page": limit,
            "total_rows": total,
            "total_pages": total_pages,
        },
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


async def get_chemicals(
    session: AsyncSession, common_name: str, page: int = 1, limit: int = _DEFAULT_LIMIT
) -> dict[str, object]:
    """Chemicals measured for this bioactivity (most-active first), paginated.

    Paginated because large bioactivities have tens of thousands of chemicals and
    each row carries its measurements — returning all at once produces 50-100MB
    responses that time out (502).
    """
    limit = max(1, min(limit, _MAX_LIMIT))
    page = max(page, 1)
    result = await session.execute(
        text("""
            SELECT chemical_name AS name, chemical_foodatlas_id AS id,
                   measurement_count, active_count, inactive_count,
                   unspecified_count, inconclusive_count, measurements
            FROM mv_chemical_bioactivity WHERE bioactivity_name = :name
            ORDER BY active_count DESC, measurement_count DESC
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {"name": common_name, "offset": limit * (page - 1), "limit": limit},
    )
    data = [dict(row._mapping) for row in result]
    total = (
        await session.execute(
            text(
                "SELECT COUNT(*) FROM mv_chemical_bioactivity"
                " WHERE bioactivity_name = :name"
            ),
            {"name": common_name},
        )
    ).scalar() or 0
    return _page_envelope(data, page, limit, total)


async def get_foods(
    session: AsyncSession, common_name: str, page: int = 1, limit: int = _DEFAULT_LIMIT
) -> dict[str, object]:
    """Foods that exhibit this bioactivity, paginated (mirrors get_chemicals)."""
    limit = max(1, min(limit, _MAX_LIMIT))
    page = max(page, 1)
    result = await session.execute(
        text("""
            SELECT food_name AS name, food_foodatlas_id AS id,
                   measurement_count, measurements
            FROM mv_food_bioactivity WHERE bioactivity_name = :name
            ORDER BY measurement_count DESC
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {"name": common_name, "offset": limit * (page - 1), "limit": limit},
    )
    data = [dict(row._mapping) for row in result]
    total = (
        await session.execute(
            text(
                "SELECT COUNT(*) FROM mv_food_bioactivity"
                " WHERE bioactivity_name = :name"
            ),
            {"name": common_name},
        )
    ).scalar() or 0
    return _page_envelope(data, page, limit, total)


async def get_chemical_bioactivities(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """A chemical's bioactivities + the measurements behind each."""
    result = await session.execute(
        text("""
            SELECT bioactivity_name AS name, bioactivity_foodatlas_id AS id,
                   measurement_count, active_count, inactive_count,
                   unspecified_count, inconclusive_count, measurements
            FROM mv_chemical_bioactivity WHERE chemical_name = :name
            ORDER BY active_count DESC, measurement_count DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_food_bioactivities(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """A food's bioactivities."""
    result = await session.execute(
        text("""
            SELECT bioactivity_name AS name, bioactivity_foodatlas_id AS id,
                   measurement_count, measurements
            FROM mv_food_bioactivity WHERE food_name = :name
            ORDER BY measurement_count DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}
