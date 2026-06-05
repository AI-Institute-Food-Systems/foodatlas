"""Bioactivity queries for /v1/.

Surfaces three MVs that the DB layer materialises:

- ``mv_chemical_bioactivity_measurement`` — r5 assay rows
- ``mv_food_bioactivity_exhibits``        — r6 direct/inherited rows
- ``mv_bioactivity_disease_association``  — r7 polarity + target rows

For ``BioactivityMeasurementRow`` we expose the *first* measurement's
potency/Hill-curve so the row stays flat; ``measurement_count`` signals
when more detail lives in the SSR-only endpoint.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from .pagination import offset as _offset

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


_MEASUREMENT_SELECT = """
    chemical_foodatlas_id AS chemical_id,
    chemical_name,
    bioactivity_foodatlas_id AS bioactivity_id,
    bioactivity_name,
    measurement_count,
    COALESCE(measurements->0->'potency', '{}'::jsonb) AS potency,
    COALESCE(measurements->0->'hill_curve', '{}'::jsonb) AS hill_curve,
    COALESCE(
        (
            SELECT array_agg(DISTINCT tid)
            FROM jsonb_array_elements(measurements) AS m,
                 jsonb_array_elements_text(
                     COALESCE(m->'target_ids', '[]'::jsonb)
                 ) AS tid
        ),
        ARRAY[]::text[]
    ) AS target_ids,
    measurements->0->>'evidence_source' AS evidence_source,
    measurements->0->>'evidence_type' AS evidence_type
"""


async def list_measurements(
    session: AsyncSession,
    *,
    chemical_id: str | None = None,
    bioactivity_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List chemical-bioactivity measurements from either side."""
    if chemical_id is not None:
        where = "chemical_foodatlas_id = :v"
        order = "bioactivity_name"
        params: dict[str, object] = {"v": chemical_id}
    elif bioactivity_id is not None:
        where = "bioactivity_foodatlas_id = :v"
        order = "chemical_name"
        params = {"v": bioactivity_id}
    else:
        return [], 0

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_chemical_bioactivity_measurement WHERE {where}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = (
        f"SELECT {_MEASUREMENT_SELECT} "
        f"FROM mv_chemical_bioactivity_measurement WHERE {where} "
        f"ORDER BY {order} OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY"
    )
    list_result = await session.execute(text(sql), params)
    rows = [dict(r._mapping) for r in list_result]
    return rows, total


_EXHIBIT_SELECT = """
    food_foodatlas_id AS food_id,
    food_name,
    bioactivity_foodatlas_id AS bioactivity_id,
    bioactivity_name,
    exhibit_type,
    via_chemical_id,
    via_chemical_name,
    efficacy_pred,
    evidence_count
"""


async def list_exhibits(
    session: AsyncSession,
    *,
    food_id: str | None = None,
    bioactivity_id: str | None = None,
    exhibit_type: str = "all",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List food-bioactivity exhibits with optional direct/inherited filter."""
    where: list[str] = []
    params: dict[str, object] = {}
    order = "evidence_count DESC"
    if food_id is not None:
        where.append("food_foodatlas_id = :v")
        params["v"] = food_id
        order = "bioactivity_name"
    elif bioactivity_id is not None:
        where.append("bioactivity_foodatlas_id = :v")
        params["v"] = bioactivity_id
        order = "food_name"
    else:
        return [], 0
    if exhibit_type in ("direct", "inherited"):
        where.append("exhibit_type = :et")
        params["et"] = exhibit_type

    where_sql = " AND ".join(where)

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_food_bioactivity_exhibits WHERE {where_sql}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = (
        f"SELECT {_EXHIBIT_SELECT} "
        f"FROM mv_food_bioactivity_exhibits WHERE {where_sql} "
        f"ORDER BY {order} OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY"
    )
    list_result = await session.execute(text(sql), params)
    rows = [dict(r._mapping) for r in list_result]
    return rows, total


_ASSOCIATION_SELECT = """
    bioactivity_foodatlas_id AS bioactivity_id,
    bioactivity_name,
    disease_foodatlas_id AS disease_id,
    disease_name,
    polarity,
    COALESCE(target_ids, ARRAY[]::text[]) AS target_ids,
    evidence_count
"""


async def list_associations(
    session: AsyncSession,
    *,
    bioactivity_id: str | None = None,
    disease_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List bioactivity-disease associations from either side."""
    if bioactivity_id is not None:
        where = "bioactivity_foodatlas_id = :v"
        order = "disease_name"
        params: dict[str, object] = {"v": bioactivity_id}
    elif disease_id is not None:
        where = "disease_foodatlas_id = :v"
        order = "bioactivity_name"
        params = {"v": disease_id}
    else:
        return [], 0

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_bioactivity_disease_association WHERE {where}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = (
        f"SELECT {_ASSOCIATION_SELECT} "
        f"FROM mv_bioactivity_disease_association WHERE {where} "
        f"ORDER BY {order} OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY"
    )
    list_result = await session.execute(text(sql), params)
    rows = [dict(r._mapping) for r in list_result]
    return rows, total
