"""Taxonomy ancestry via IS_A (r2) hierarchy traversal."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# IS_A direction per entity type: (child_column, parent_column).
# Food & Disease: head=child, tail=parent.
# Chemical: head=parent, tail=child.
_DIRECTION: dict[str, tuple[str, str]] = {
    "food": ("head_id", "tail_id"),
    "chemical": ("tail_id", "head_id"),
    "disease": ("head_id", "tail_id"),
}

_MV_TABLE: dict[str, str] = {
    "food": "mv_food_entities",
    "chemical": "mv_chemical_entities",
    "disease": "mv_disease_entities",
}

_MAX_DEPTH = 20


async def get_taxonomy(
    session: AsyncSession,
    common_name: str,
    entity_type: str,
) -> dict[str, object]:
    """Return the IS_A ancestor tree for an entity.

    Walks upward from the entity through r2 triplets, returning all
    ancestor nodes and parent-child edges.
    """
    entity_id = await _resolve_entity_id(session, common_name, entity_type)
    if entity_id is None:
        return {"data": {"entity_id": None, "nodes": [], "edges": []}}

    child_col, parent_col = _DIRECTION[entity_type]
    sql = _build_ancestry_sql(child_col, parent_col)

    result = await session.execute(text(sql), {"entity_id": entity_id})
    rows = [dict(r._mapping) for r in result]

    nodes_seen: dict[str, str] = {}
    edges: list[dict[str, str]] = []
    for row in rows:
        nid = row["node_id"]
        name = row["node_name"]
        came_from = row["came_from_id"]
        if nid not in nodes_seen:
            nodes_seen[nid] = name
        if came_from is not None:
            nodes_seen.setdefault(came_from, row.get("came_from_name", ""))
            edges.append({"child_id": came_from, "parent_id": nid})

    # Check which ancestor nodes have entity pages in the MV table.
    page_ids = await _find_ids_with_pages(
        session,
        entity_type,
        set(nodes_seen.keys()),
    )

    nodes = [
        {"id": nid, "name": name, "has_page": nid in page_ids}
        for nid, name in nodes_seen.items()
    ]

    return {
        "data": {
            "entity_id": entity_id,
            "nodes": nodes,
            "edges": edges,
        }
    }


async def _resolve_entity_id(
    session: AsyncSession,
    common_name: str,
    entity_type: str,
) -> str | None:
    """Look up foodatlas_id from the materialized entity view."""
    table = _MV_TABLE[entity_type]
    result = await session.execute(
        text(f"SELECT foodatlas_id FROM {table} WHERE common_name = :name"),
        {"name": common_name},
    )
    row = result.first()
    return row[0] if row else None


async def _find_ids_with_pages(
    session: AsyncSession,
    entity_type: str,
    ids: set[str],
) -> set[str]:
    """Return the subset of *ids* that exist in the MV entity table."""
    if not ids:
        return set()
    table = _MV_TABLE[entity_type]
    result = await session.execute(
        text(f"SELECT foodatlas_id FROM {table} WHERE foodatlas_id = ANY(:ids)"),
        {"ids": list(ids)},
    )
    return {row[0] for row in result}


def _build_ancestry_sql(child_col: str, parent_col: str) -> str:
    """Build the recursive CTE for walking IS_A ancestors.

    Column names come from an internal allowlist, not user input.
    """
    return f"""
        WITH RECURSIVE ancestry AS (
            SELECT
                be.foodatlas_id AS node_id,
                be.common_name  AS node_name,
                NULL::VARCHAR   AS came_from_id,
                NULL::VARCHAR   AS came_from_name,
                0               AS depth,
                ARRAY[be.foodatlas_id]::TEXT[] AS visited
            FROM base_entities be
            WHERE be.foodatlas_id = :entity_id

            UNION ALL

            SELECT
                p.foodatlas_id  AS node_id,
                p.common_name   AS node_name,
                a.node_id       AS came_from_id,
                a.node_name     AS came_from_name,
                a.depth + 1,
                a.visited || p.foodatlas_id
            FROM ancestry a
            JOIN base_triplets bt
                ON bt.{child_col} = a.node_id
                AND bt.relationship_id = 'r2'
            JOIN base_entities p
                ON p.foodatlas_id = bt.{parent_col}
            WHERE p.foodatlas_id != ALL(a.visited)
              AND a.depth < {_MAX_DEPTH}
        )
        SELECT node_id, node_name, came_from_id, came_from_name
        FROM ancestry
    """
