"""Generate display-friendly synonym lists for entities."""

import logging

import pandas as pd
from inflection import pluralize, singularize

from ..stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def remove_plural_synonyms(synonyms: list[str]) -> list[str]:
    """Remove plural suffix forms from a FoodOn synonym list.

    The heuristic assumes plurals were appended at the end of the list.
    It searches for the first synonym whose plural/singular form appears
    later, and truncates there.
    """
    if len(synonyms) <= 1:
        return list(synonyms)

    for i, s in enumerate(synonyms):
        sp = pluralize(s)
        try:
            i_end = synonyms[i + 1 :].index(sp) + i + 1
            return synonyms[:i_end]
        except ValueError:
            pass

        ss = singularize(s)
        try:
            i_end = synonyms[i + 1 :].index(ss) + i + 1
            return synonyms[:i_end]
        except ValueError:
            continue

    return list(synonyms)


def build_synonyms_display(
    entities: pd.DataFrame,
    mesh: pd.DataFrame | None = None,
) -> pd.Series:
    """Build a ``_synonyms_display`` dict for each entity.

    Args:
        entities: Entity DataFrame with ``synonyms``, ``external_ids``,
            and ``entity_type`` columns.
        mesh: Optional MeSH categories DataFrame (indexed by ``id``).
            When provided, MeSH synonym names are included for chemicals
            with MeSH external IDs.

    Returns:
        Series of ``dict[str, list[str]]`` keyed by ontology source.
    """
    return entities.apply(
        lambda row: _build_row(row, mesh),
        axis=1,
    )


def _build_row(
    row: pd.Series,
    mesh: pd.DataFrame | None,
) -> dict[str, list[str]]:
    """Build synonyms_display for a single entity row."""
    result: dict[str, list[str]] = {}

    if row["entity_type"] == "food":
        if "foodon" in row["external_ids"]:
            result["foodon"] = remove_plural_synonyms(row["synonyms"])

    elif row["entity_type"] == "chemical":
        if "chebi" in row["external_ids"]:
            result["chebi"] = list(row["synonyms"])

        if "mesh" in row["external_ids"] and mesh is not None:
            mesh_id = row["external_ids"]["mesh"][0]
            if mesh_id in mesh.index:
                mesh_synonyms = mesh.loc[mesh_id]
                if isinstance(mesh_synonyms, pd.Series) and "synonyms" in mesh_synonyms:
                    syns = mesh_synonyms["synonyms"]
                    result["mesh"] = syns if isinstance(syns, list) else [syns]

    return result


def apply_synonyms_display(
    entity_store: EntityStore,
    mesh: pd.DataFrame | None = None,
) -> None:
    """Update ``_synonyms_display`` on all entities in *entity_store*."""
    entities = entity_store._entities
    entities["_synonyms_display"] = build_synonyms_display(entities, mesh)
    logger.info("Updated synonyms_display for %d entities.", len(entities))
