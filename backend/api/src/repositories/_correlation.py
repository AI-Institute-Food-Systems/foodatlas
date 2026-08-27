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

# What makes one rendered row. Both ends group by the same tuple: the
# source chemical stays in the key because a class page shows one row per
# descendant that contributes the connection.
GROUP_BY_PAIR = (
    "disease_foodatlas_id, disease_name, "
    "source_chemical_name, source_chemical_foodatlas_id, "
    "chemical_foodatlas_id, chemical_name"
)

# r4 helps reduce the disease, r3 worsens it. "all" maps to neither, which
# is what lets one page carry both directions.
RELATION_IDS = {"positive": "r4", "negative": "r3"}

# Direction as the UI names it, keyed by relationship_id.
IMPROVES = "r4"
WORSENS = "r3"

# Per-direction evidence for one (anchor, source_chemical, peer) pair,
# plus the direction list. The view stores one row per direction, so a
# pair that has been reported both ways is two rows; the UI shows it as
# one row with a "mixed" glyph, which means the grouping has to happen in
# SQL. Doing it per page in the frontend would be wrong: the two halves
# are ordered by evidence_count and routinely land on different pages.
#
# `(array_agg(x) FILTER (WHERE ...))[1]` is safe because the materializer
# emits exactly one row per (chemical, source_chemical, disease, rel) —
# verified against the snapshot, max rows per direction is 1.
PAIR_AGGREGATES = (
    "array_agg(DISTINCT relationship_id) AS relationship_ids, "
    f"(array_agg(evidences) FILTER (WHERE relationship_id = '{IMPROVES}'))[1] "
    "AS improves_evidences, "
    f"(array_agg(evidences) FILTER (WHERE relationship_id = '{WORSENS}'))[1] "
    "AS worsens_evidences, "
    "SUM(evidence_count) AS evidence_count"
)


def _evidence_key(evidence: dict) -> str:
    pmid = (evidence.get("pmid") or {}).get("id")
    pmcid = (evidence.get("pmcid") or {}).get("id")
    return str(pmid or pmcid or "")


def merge_evidences(improves: list | None, worsens: list | None) -> list[dict]:
    """Union of both directions' evidence, deduped by PMID/PMCID.

    A single paper can be cited for both directions — it reports a
    benefit in one context and a harm in another. Concatenating without
    deduping would make the row's "See N publications" button overcount
    exactly those pairs.
    """
    merged: list[dict] = []
    seen: set[str] = set()
    for evidence in (*(improves or []), *(worsens or [])):
        key = _evidence_key(evidence)
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        merged.append(evidence)
    return merged


def shape_pair_rows(rows: list[dict]) -> list[dict]:
    """Attach the deduped union to each grouped row."""
    for row in rows:
        row["evidences"] = merge_evidences(
            row.get("improves_evidences"), row.get("worsens_evidences")
        )
    return rows


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

    These count PAIRS, matching what the table renders after grouping, so
    "All" is not improves + worsens: the ~4% of pairs reported both ways
    are one row there and are counted once here. Summing the two would
    overshoot the row count the user is about to see.
    """
    where, params = build_filters("all", search, peer_column)
    result = await session.execute(
        text(f"""
            SELECT
              COUNT(*) FILTER (WHERE has_improves) AS improves,
              COUNT(*) FILTER (WHERE has_worsens) AS worsens,
              COUNT(*) AS both
            FROM (
              SELECT
                bool_or(relationship_id = '{IMPROVES}') AS has_improves,
                bool_or(relationship_id = '{WORSENS}') AS has_worsens
              FROM {VIEW}
              WHERE {anchor_column} = :name{where}
              GROUP BY {GROUP_BY_PAIR}
            ) pairs
        """),
        {"name": common_name, **params},
    )
    row = result.one()
    return {
        "improves": int(row.improves),
        "worsens": int(row.worsens),
        "both": int(row.both),
    }
