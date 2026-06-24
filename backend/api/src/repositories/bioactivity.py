"""Bioactivity repository — queries the bioactivity materialized views.

Reads only ``mv_bioactivity_entities`` / ``mv_chemical_bioactivity`` /
``mv_food_bioactivity`` (never base tables), matching the rest of the API.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


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


async def get_chemicals(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Chemicals measured for this bioactivity (most-active first)."""
    result = await session.execute(
        text("""
            SELECT chemical_name AS name, chemical_foodatlas_id AS id,
                   measurement_count, active_count, inactive_count,
                   unspecified_count, inconclusive_count, measurements
            FROM mv_chemical_bioactivity WHERE bioactivity_name = :name
            ORDER BY active_count DESC, measurement_count DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_foods(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Foods that exhibit this bioactivity."""
    result = await session.execute(
        text("""
            SELECT food_name AS name, food_foodatlas_id AS id,
                   measurement_count, measurements
            FROM mv_food_bioactivity WHERE bioactivity_name = :name
            ORDER BY measurement_count DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


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
