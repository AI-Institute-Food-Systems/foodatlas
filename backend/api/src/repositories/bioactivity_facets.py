"""Sidebar facet counts for the bioactivity tables.

Every facet counts table ROWS, applies every other active filter, and uses
the same predicates as the paginated queries in ``bioactivity.py``: one
shown assay must match Evidence, Unit and Source together. So each number
equals the table total after picking that value.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

from src.repositories._search_util import build_ilike_pattern
from src.repositories.bioactivity_counts import (
    assay_exists_sql,
    key_conditions,
    key_evidence,
    key_unit,
    split_multi,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

INFERRED = "food-inferred-bioactivities"

# direction → (mv, pivot name column, row search column)
DIRECTIONS: dict[str, tuple[str, str, str]] = {
    "bioactivity-chemicals": (
        "mv_chemical_bioactivity",
        "bioactivity_name",
        "chemical_name",
    ),
    "bioactivity-foods": ("mv_food_bioactivity", "bioactivity_name", "food_name"),
    "chemical-bioactivities": (
        "mv_chemical_bioactivity",
        "chemical_name",
        "bioactivity_name",
    ),
    "food-bioactivities": ("mv_food_bioactivity", "food_name", "bioactivity_name"),
}

CATEGORY_SQL = (
    "EXISTS (SELECT 1 FROM mv_chemical_entities e"
    " WHERE e.foodatlas_id = {alias}chemical_foodatlas_id"
    " AND e.chemical_classification && :cats)"
)


@dataclass(frozen=True)
class PageFilters:
    search: str = ""
    evidence_type: str = ""
    unit: str = ""
    source_kind: str = ""
    category: str = ""


@dataclass(frozen=True)
class _Scope:
    from_sql: str
    alias: str
    row_id: str
    where: list[str]
    params: dict


def _scope(
    common_name: str, direction: str, f: PageFilters, skip: str
) -> _Scope | None:
    """FROM, row identity and the non-assay WHERE of one table, minus ``skip``."""
    params: dict = {"name": common_name}
    pattern = build_ilike_pattern(f.search)
    if direction == INFERRED:
        alias = "cb."
        from_sql = (
            "mv_food_chemical_composition fcc JOIN mv_chemical_bioactivity cb"
            " ON cb.chemical_foodatlas_id = fcc.chemical_foodatlas_id"
        )
        row_id = "(fcc.ctid, cb.ctid)"
        where = ["fcc.food_name = :name"]
        if pattern:
            where.append("(cb.bioactivity_name ILIKE :q OR fcc.chemical_name ILIKE :q)")
            params["q"] = pattern
        return _Scope(from_sql, alias, row_id, where, params)
    if direction not in DIRECTIONS:
        return None
    mv, name_col, search_col = DIRECTIONS[direction]
    where = [f"mv.{name_col} = :name"]
    if pattern:
        where.append(f"mv.{search_col} ILIKE :q")
        params["q"] = pattern
    cats = split_multi(f.category)
    if cats and skip != "category" and direction == "bioactivity-chemicals":
        where.append(CATEGORY_SQL.format(alias="mv."))
        params["cats"] = cats
    return _Scope(f"{mv} mv", "mv.", "mv.ctid", where, params)


def _key_conds(f: PageFilters, skip: str) -> tuple[list[str], dict]:
    return key_conditions(
        "k.key",
        evidence_type=f.evidence_type,
        unit=f.unit,
        source_kind=f.source_kind,
        skip=skip,
    )


async def _grouped(
    session: AsyncSession,
    common_name: str,
    direction: str,
    f: PageFilters,
    dim: str,
    label_sql: str,
) -> list[tuple[str, int]]:
    """(value, rows) for one assay dimension, other filters applied."""
    scope = _scope(common_name, direction, f, dim)
    if scope is None:
        return []
    conds, params = _key_conds(f, dim)
    result = await session.execute(
        text(f"""
            SELECT label, COUNT(DISTINCT rid) AS count
            FROM (
                SELECT {scope.row_id} AS rid, {label_sql} AS label
                FROM {scope.from_sql}
                CROSS JOIN LATERAL unnest({scope.alias}assay_keys) AS k(key)
                WHERE {" AND ".join(scope.where + conds)}
            ) AS x
            WHERE label <> ''
            GROUP BY label
            ORDER BY count DESC, label
        """),
        {**scope.params, **params},
    )
    return [(r.label, int(r[1])) for r in result]


async def get_evidence_type_counts(
    session: AsyncSession, common_name: str, direction: str, f: PageFilters
) -> dict[str, object]:
    rows = await _grouped(
        session, common_name, direction, f, "evidence", key_evidence("k.key")
    )
    data = [{"evidence_type": label, "count": n} for label, n in rows]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_unit_counts(
    session: AsyncSession, common_name: str, direction: str, f: PageFilters
) -> dict[str, object]:
    rows = await _grouped(session, common_name, direction, f, "unit", key_unit("k.key"))
    data = [{"endpoint": "", "unit": label, "count": n} for label, n in rows]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_source_kind_counts(
    session: AsyncSession, common_name: str, direction: str, f: PageFilters
) -> dict[str, object]:
    empty: dict[str, object] = {"data": {"both": 0, "experimental": 0, "predicted": 0}}
    scope = _scope(common_name, direction, f, "source")
    if scope is None:
        return empty
    conds, params = _key_conds(f, "source")
    keys = f"{scope.alias}assay_keys"
    both = assay_exists_sql(keys, conds)
    exp_conds, _ = key_conditions(
        "k.key", evidence_type=f.evidence_type, unit=f.unit, source_kind="experimental"
    )
    pred_conds, _ = key_conditions(
        "k.key", evidence_type=f.evidence_type, unit=f.unit, source_kind="predicted"
    )
    exp_sql = assay_exists_sql(keys, exp_conds)
    pred_sql = assay_exists_sql(keys, pred_conds)
    result = await session.execute(
        text(f"""
            SELECT
                COUNT(*) AS both,
                COUNT(*) FILTER (WHERE {exp_sql}) AS experimental,
                COUNT(*) FILTER (WHERE {pred_sql}) AS predicted
            FROM {scope.from_sql}
            WHERE {" AND ".join([*scope.where, both])}
        """),
        {**scope.params, **params},
    )
    row = result.first()
    if row is None:
        return empty
    return {
        "data": {
            "both": int(row.both or 0),
            "experimental": int(row.experimental or 0),
            "predicted": int(row.predicted or 0),
        }
    }


async def get_category_counts(
    session: AsyncSession, common_name: str, f: PageFilters
) -> dict[str, object]:
    """Chemical-classification row counts on the bioactivity's Chemicals tab."""
    scope = _scope(common_name, "bioactivity-chemicals", f, "category")
    if scope is None:
        return {"data": [], "metadata": {"row_count": 0}}
    conds, params = _key_conds(f, "")
    where = [*scope.where, assay_exists_sql("mv.assay_keys", conds)]
    result = await session.execute(
        text(f"""
            SELECT category, COUNT(*) AS count
            FROM {scope.from_sql}
            JOIN mv_chemical_entities ce ON ce.foodatlas_id = mv.chemical_foodatlas_id
            CROSS JOIN LATERAL UNNEST(ce.chemical_classification) AS category
            WHERE category <> '' AND {" AND ".join(where)}
            GROUP BY category
            ORDER BY COUNT(*) DESC, category
        """),
        {**scope.params, **params},
    )
    data = [{"category": r.category, "count": int(r[1])} for r in result]
    return {"data": data, "metadata": {"row_count": len(data)}}
