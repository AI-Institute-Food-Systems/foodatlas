"""Shared query pieces for the two CTD correlation directions.

``mv_chemical_disease_correlation`` is read from both ends: the chemical
page anchors on ``chemical_name`` and lists diseases, the disease page
anchors on ``disease_name`` and lists chemicals. The direction filter,
the search predicate and the facet counts are identical either way, so
they live here rather than being written twice and drifting.

Column names reaching the f-strings below are module constants chosen by
the caller, never user input — the same static-fragment interpolation
``food.py`` uses for BASE_SELECT. Everything derived from a request is
bound as a parameter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from ._search_util import build_ilike_pattern

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

VIEW = "mv_chemical_disease_correlation"

# r4 helps reduce the disease, r3 worsens it. "all" maps to neither, which
# is what lets one page carry both directions.
RELATION_IDS = {"positive": "r4", "negative": "r3"}

# Direction as the UI names it, keyed by relationship_id.
IMPROVES = "r4"
WORSENS = "r3"


def build_filters(
    relation: str, search: str, peer_column: str
) -> tuple[str, dict[str, object]]:
    """WHERE tail shared by the page, count and facet queries.

    Returns the SQL fragment (each clause pre-prefixed with " AND ") and
    its bind parameters. An unrecognised ``relation`` — including "all" —
    drops the direction predicate, so both r3 and r4 rows come back in
    one page and the frontend renders the direction per row instead of
    running a table per direction.

    ``peer_column`` must be qualified to match the query it is spliced
    into: the disease-side page query aliases the view as ``c``, its
    count query does not.
    """
    clauses = ""
    params: dict[str, object] = {}

    relationship_id = RELATION_IDS.get(relation)
    if relationship_id is not None:
        clauses += " AND relationship_id = :rel"
        params["rel"] = relationship_id

    # None for blank/whitespace-only input, so the clause and its bind
    # parameter are both skipped rather than becoming ILIKE '% %'.
    pattern = build_ilike_pattern(search)
    if pattern is not None:
        clauses += f" AND {peer_column} ILIKE :pattern"
        params["pattern"] = pattern

    return clauses, params


async def get_direction_counts(
    session: AsyncSession,
    anchor_column: str,
    peer_column: str,
    common_name: str,
    search: str = "",
) -> dict[str, int]:
    """Improves/worsens row counts for the merged tab's Direction facet.

    Counted under the active search but NOT under the active direction —
    a facet that only counted the direction already selected would read
    zero for the option the user is trying to switch to.
    """
    where, params = build_filters("all", search, peer_column)
    result = await session.execute(
        text(f"""
            SELECT relationship_id, COUNT(*) AS n
            FROM {VIEW}
            WHERE {anchor_column} = :name{where}
            GROUP BY relationship_id
        """),
        {"name": common_name, **params},
    )
    by_relation = {row.relationship_id: int(row.n) for row in result}
    improves = by_relation.get(IMPROVES, 0)
    worsens = by_relation.get(WORSENS, 0)
    return {
        "improves": improves,
        "worsens": worsens,
        "both": improves + worsens,
    }
