"""Flat entity queries for /v1/ (food, chemical, disease, bioactivity).

Reuses the existing materialised views (``mv_food_entities``,
``mv_chemical_entities``, ``mv_disease_entities``, ``mv_bioactivity_entities``)
but returns the columns flat — no UI fields like ``ambiguity_siblings``, no
external-id reformatting. External IDs are returned as-is from the source
data so consumers can map them however they like.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from .pagination import offset as _offset

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_ENTITY_TABLE: dict[str, str] = {
    "food": "mv_food_entities",
    "chemical": "mv_chemical_entities",
    "disease": "mv_disease_entities",
    "bioactivity": "mv_bioactivity_entities",
}

_ENTITY_SELECT: dict[str, str] = {
    "food": (
        "foodatlas_id AS id, common_name, scientific_name, synonyms, "
        "external_ids, food_classification"
    ),
    "chemical": (
        "foodatlas_id AS id, common_name, scientific_name, synonyms, "
        "external_ids, chemical_classification, flavor_descriptors"
    ),
    "disease": (
        "foodatlas_id AS id, common_name, scientific_name, synonyms, external_ids"
    ),
    "bioactivity": (
        "foodatlas_id AS id, common_name, synonyms, external_ids, "
        "description, parents, children, n_foods, n_chemicals"
    ),
}


async def list_entities(
    session: AsyncSession,
    entity_type: str,
    *,
    q: str = "",
    classification: str = "",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """Return (rows, total) for an entity list.

    ``q`` is a case-insensitive substring filter on ``common_name``.
    ``classification`` is an exact-membership filter on the per-type
    classification array (food: ``food_classification``;
    chemical: ``chemical_classification``; ignored for disease).
    """
    table = _ENTITY_TABLE[entity_type]
    cols = _ENTITY_SELECT[entity_type]

    where: list[str] = []
    params: dict[str, object] = {}
    if q:
        where.append("common_name ILIKE :q")
        params["q"] = f"%{q}%"
    if classification and entity_type in ("food", "chemical"):
        col = (
            "food_classification"
            if entity_type == "food"
            else "chemical_classification"
        )
        where.append(f":cls = ANY({col})")
        params["cls"] = classification

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    count_sql = f"SELECT COUNT(*) FROM {table}{where_sql}"
    count_result = await session.execute(text(count_sql), params)
    total = int(count_result.scalar() or 0)

    params["limit"] = page_size
    params["offset"] = _offset(page, page_size)
    list_sql = (
        f"SELECT {cols} FROM {table}{where_sql} ORDER BY common_name "
        "OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY"
    )
    list_result = await session.execute(text(list_sql), params)
    rows = [dict(r._mapping) for r in list_result]
    return rows, total


async def get_entity(
    session: AsyncSession,
    entity_type: str,
    *,
    entity_id: str | None = None,
    common_name: str | None = None,
) -> dict | None:
    """Look up a single entity by foodatlas_id or common_name (exact)."""
    table = _ENTITY_TABLE[entity_type]
    cols = _ENTITY_SELECT[entity_type]
    if entity_id is not None:
        sql = f"SELECT {cols} FROM {table} WHERE foodatlas_id = :v"
        params: dict[str, object] = {"v": entity_id}
    elif common_name is not None:
        sql = f"SELECT {cols} FROM {table} WHERE common_name = :v"
        params = {"v": common_name}
    else:
        return None
    result = await session.execute(text(sql), params)
    row = result.first()
    return dict(row._mapping) if row else None


async def resolve_id(session: AsyncSession, entity_id: str) -> tuple[str, str] | None:
    """Return (entity_type, common_name) for a foodatlas_id, or None."""
    result = await session.execute(
        text(
            "SELECT entity_type, common_name FROM base_entities "
            "WHERE foodatlas_id = :id"
        ),
        {"id": entity_id},
    )
    row = result.first()
    return (row[0], row[1]) if row else None
