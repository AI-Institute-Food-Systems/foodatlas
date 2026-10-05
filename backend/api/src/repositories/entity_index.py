"""The entity index: every entity that has a page on the website.

Feeds the frontend sitemap (``/metadata/entities``). The rule here must match
the one the frontend uses to 404 an entity route (``requireEntity.ts``):

* a row in the type's metadata MV (``mv_{type}_entities``) has a page;
* a chemical with no metadata row still has a page when it has bioassay
  measurements (``mv_chemical_bioactivity``).

Reading ``mv_search_auto_complete`` instead left out about two thirds of the
pages that return 200 — the bioassay-only chemicals, and the chemicals and
diseases that are only reached through correlations or ancestry.
"""

from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

IndexEntityType = Literal["food", "chemical", "disease", "bioactivity"]

# (entity_type, tier, select). Tier 0 is the metadata MV; tier 1 is the
# metadata-less exception. Within a type the sitemap lists tier 0 first, so
# a cap on the sitemap size drops the thinnest pages, not the richest.
_SOURCES: tuple[tuple[IndexEntityType, int, str], ...] = (
    ("food", 0, "SELECT foodatlas_id, common_name FROM mv_food_entities"),
    ("chemical", 0, "SELECT foodatlas_id, common_name FROM mv_chemical_entities"),
    (
        "chemical",
        1,
        "SELECT chemical_foodatlas_id, chemical_name FROM mv_chemical_bioactivity",
    ),
    ("disease", 0, "SELECT foodatlas_id, common_name FROM mv_disease_entities"),
    (
        "bioactivity",
        0,
        "SELECT foodatlas_id, common_name FROM mv_bioactivity_entities",
    ),
)


def _index_sql(entity_type: IndexEntityType | None) -> str:
    # Every fragment is a literal from _SOURCES, filtered by a validated
    # Literal — no request input reaches the SQL text.
    parts = [
        f"SELECT '{etype}' AS entity_type, {tier} AS tier, s.* FROM ({select}) s"
        for etype, tier, select in _SOURCES
        if entity_type is None or etype == entity_type
    ]
    union = "\n  UNION ALL\n  ".join(parts)
    # One URL per (type, name): names are the slug, so two entities that
    # share a name share a page. Keep the lowest tier for each name.
    return f"""
        WITH pages(entity_type, tier, foodatlas_id, common_name) AS (
          {union}
        ),
        named AS (
          SELECT DISTINCT ON (entity_type, common_name)
                 foodatlas_id, entity_type, common_name, tier
          FROM pages
          WHERE common_name <> ''
          ORDER BY entity_type, common_name, tier, foodatlas_id
        )
        SELECT foodatlas_id, entity_type, common_name
        FROM named
        ORDER BY entity_type, tier, common_name
    """


async def list_entities(
    session: AsyncSession, entity_type: IndexEntityType | None = None
) -> list[dict[str, str]]:
    """Every entity with a page, as (foodatlas_id, entity_type, common_name).

    ``entity_type`` limits the index to one type, so each per-type sitemap
    fetches only its own rows.
    """
    result = await session.execute(text(_index_sql(entity_type)))
    return [dict(row._mapping) for row in result]
