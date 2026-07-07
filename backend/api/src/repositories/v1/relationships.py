"""Flat composition + correlation queries for /v1/.

Both endpoints return the same flat row shape regardless of which side
("food's chemicals" vs. "chemical's foods") the caller asked for. Sources
are aggregated into a single list rather than the UI's
fdc/foodatlas-grouped evidence arrays.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from .pagination import offset as _offset

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_RELATION_TO_ID: dict[str, str] = {"reduces": "r4", "worsens": "r3"}


_COMPOSITION_SELECT_CLAUSE = """
    food_foodatlas_id AS food_id,
    food_name,
    chemical_foodatlas_id AS chemical_id,
    chemical_name,
    chemical_classification,
    median_concentration,
    COALESCE(jsonb_array_length(fdc_evidences), 0)
        + COALESCE(jsonb_array_length(foodatlas_evidences), 0) AS attestation_count,
    ARRAY_REMOVE(
        ARRAY[
            CASE WHEN fdc_evidences IS NOT NULL THEN 'fdc' END,
            CASE WHEN foodatlas_evidences IS NOT NULL THEN 'foodatlas' END
        ],
        NULL
    ) AS sources
"""


async def list_composition(
    session: AsyncSession,
    *,
    food_id: str | None = None,
    chemical_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List composition rows filtered by either food_id or chemical_id."""
    if food_id is not None:
        where = "food_foodatlas_id = :v"
        order = "chemical_name"
        params: dict[str, object] = {"v": food_id}
    elif chemical_id is not None:
        where = "chemical_foodatlas_id = :v"
        order = "food_name"
        params = {"v": chemical_id}
    else:
        return [], 0

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_food_chemical_composition WHERE {where}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = (
        f"SELECT {_COMPOSITION_SELECT_CLAUSE} "
        f"FROM mv_food_chemical_composition WHERE {where} "
        f"ORDER BY {order} OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY"
    )
    list_result = await session.execute(text(sql), params)
    rows = [dict(r._mapping) for r in list_result]
    return rows, total


def _top_measurement(measurements: list | None) -> dict | None:
    """Highest ``value`` sample from the MV-capped ``measurements`` array."""
    if not measurements:
        return None
    top: dict | None = None
    for m in measurements:
        v = m.get("value")
        if v is None:
            continue
        if top is None or v > top["value"]:
            top = {
                "endpoint": m.get("endpoint") or "",
                "value": v,
                "unit": m.get("unit") or "",
            }
    return top


async def list_bioactivity_chemicals(
    session: AsyncSession,
    *,
    bioactivity_id: str | None = None,
    chemical_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List chemical↔bioactivity measurement rows filtered by either side."""
    if bioactivity_id is not None:
        where = "bioactivity_foodatlas_id = :v"
        order = "measurement_count DESC NULLS LAST, chemical_name"
        params: dict[str, object] = {"v": bioactivity_id}
    elif chemical_id is not None:
        where = "chemical_foodatlas_id = :v"
        order = "measurement_count DESC NULLS LAST, bioactivity_name"
        params = {"v": chemical_id}
    else:
        return [], 0

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_chemical_bioactivity WHERE {where}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = f"""
        SELECT
            bioactivity_foodatlas_id AS bioactivity_id,
            bioactivity_name,
            chemical_foodatlas_id AS chemical_id,
            chemical_name,
            measurement_count,
            active_count,
            inactive_count,
            measurements
        FROM mv_chemical_bioactivity
        WHERE {where}
        ORDER BY {order}
        OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
    """
    list_result = await session.execute(text(sql), params)
    rows: list[dict] = []
    for r in list_result:
        row = dict(r._mapping)
        row["top_measurement"] = _top_measurement(row.pop("measurements", None))
        rows.append(row)
    return rows, total


async def list_bioactivity_foods(
    session: AsyncSession,
    *,
    bioactivity_id: str | None = None,
    food_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List food↔bioactivity measurement rows filtered by either side."""
    if bioactivity_id is not None:
        where = "bioactivity_foodatlas_id = :v"
        order = "measurement_count DESC NULLS LAST, food_name"
        params: dict[str, object] = {"v": bioactivity_id}
    elif food_id is not None:
        where = "food_foodatlas_id = :v"
        order = "measurement_count DESC NULLS LAST, bioactivity_name"
        params = {"v": food_id}
    else:
        return [], 0

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_food_bioactivity WHERE {where}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = f"""
        SELECT
            bioactivity_foodatlas_id AS bioactivity_id,
            bioactivity_name,
            food_foodatlas_id AS food_id,
            food_name,
            measurement_count,
            measurements
        FROM mv_food_bioactivity
        WHERE {where}
        ORDER BY {order}
        OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
    """
    list_result = await session.execute(text(sql), params)
    rows: list[dict] = []
    for r in list_result:
        row = dict(r._mapping)
        row["top_measurement"] = _top_measurement(row.pop("measurements", None))
        rows.append(row)
    return rows, total


async def list_correlation(
    session: AsyncSession,
    *,
    chemical_id: str | None = None,
    disease_id: str | None = None,
    relation: str = "reduces",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """List chemical-disease correlations from either side."""
    relationship_id = _RELATION_TO_ID.get(relation)
    if relationship_id is None:
        return [], 0
    if chemical_id is not None:
        where = "chemical_foodatlas_id = :v AND relationship_id = :rel"
        params: dict[str, object] = {"v": chemical_id, "rel": relationship_id}
    elif disease_id is not None:
        where = "disease_foodatlas_id = :v AND relationship_id = :rel"
        params = {"v": disease_id, "rel": relationship_id}
    else:
        return [], 0

    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_chemical_disease_correlation WHERE {where}"),
        params,
    )
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    sql = f"""
        SELECT
            chemical_foodatlas_id AS chemical_id,
            chemical_name,
            disease_foodatlas_id AS disease_id,
            disease_name,
            source_chemical_foodatlas_id AS source_chemical_id,
            source_chemical_name,
            sources,
            evidence_count
        FROM mv_chemical_disease_correlation
        WHERE {where}
        ORDER BY evidence_count DESC
        OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
    """
    list_result = await session.execute(text(sql), params)
    rows = []
    for r in list_result:
        row = dict(r._mapping)
        row["relation"] = relation
        rows.append(row)
    return rows, total
