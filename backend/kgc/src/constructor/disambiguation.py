"""Synonym disambiguation — placeholder entity creation and resolution."""

import logging
import warnings

import pandas as pd

from ..stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def _fix_scientific_name(entities: pd.DataFrame, entity_id: str) -> None:
    """Clear scientific_name if it is no longer in synonyms."""
    sci = entities.at[entity_id, "scientific_name"]
    if pd.notna(sci) and sci and sci not in entities.at[entity_id, "synonyms"]:
        entities.at[entity_id, "scientific_name"] = ""


def _remove_synonyms_and_fix_names(
    entities: pd.DataFrame,
    entity_id: str,
    synonyms_to_remove: list[str],
) -> None:
    """Remove synonyms from an entity and fix common/scientific names."""
    entities.at[entity_id, "synonyms"] = [
        s for s in entities.loc[entity_id, "synonyms"] if s not in synonyms_to_remove
    ]
    synonyms = entities.loc[entity_id, "synonyms"]

    if not synonyms:
        warnings.warn(
            f"{entities.loc[entity_id]}: Synonyms are empty.",
            stacklevel=3,
        )
        return

    if entities.at[entity_id, "common_name"] not in synonyms:
        entities.at[entity_id, "common_name"] = synonyms[0]

    _fix_scientific_name(entities, entity_id)


def _add_back_pointer(
    entities: pd.DataFrame,
    entity_id: str,
    placeholder_id: str,
) -> None:
    """Add a _placeholder_from back-pointer on an entity."""
    ext = entities.at[entity_id, "external_ids"]
    if "_placeholder_from" not in ext:
        ext["_placeholder_from"] = []
    ext["_placeholder_from"] += [placeholder_id]
    ext["_placeholder_from"] = sorted(set(ext["_placeholder_from"]))


def _handle_existing_placeholder(
    entities: pd.DataFrame,
    entity_ids: list[str],
    name: str,
    lut: dict[str, list[str]],
) -> None:
    """Extend an existing placeholder entity with new ambiguous IDs."""
    placeholder_id = entity_ids[0]
    new_ids = entity_ids[1:]
    entities.at[placeholder_id, "external_ids"]["_placeholder_to"] += new_ids
    for eid in new_ids:
        _remove_synonyms_and_fix_names(entities, eid, [name])
        _add_back_pointer(entities, eid, placeholder_id)
    lut[name] = [placeholder_id]


def _collect_new_placeholders(
    entity_store: EntityStore,
) -> list[dict]:
    """Scan LUTs for ambiguous synonyms and build placeholder rows."""
    entities = entity_store._entities
    rows: list[dict] = []

    for entity_type in ("food", "chemical"):
        lut = entity_store._get_lut(entity_type)
        for name, eids in list(lut.items()):
            if len(eids) <= 1:
                continue

            if "_placeholder_to" in entities.at[eids[0], "external_ids"]:
                _handle_existing_placeholder(entities, eids, name, lut)
            else:
                rows.append(
                    {
                        "foodatlas_id": f"e{entity_store._curr_eid}",
                        "entity_type": entity_type,
                        "common_name": name,
                        "synonyms": [name],
                        "external_ids": {"_placeholder_to": eids},
                    }
                )
                entity_store._curr_eid += 1

    return rows


def _apply_placeholder_updates(
    entity_store: EntityStore,
    placeholders: pd.DataFrame,
) -> None:
    """Update original entities to point back to their placeholders."""
    entities = entity_store._entities

    for placeholder_id, row in placeholders.iterrows():
        for eid in row["external_ids"]["_placeholder_to"]:
            _remove_synonyms_and_fix_names(entities, eid, row["synonyms"])

            if not entities.at[eid, "synonyms"]:
                continue

            _add_back_pointer(entities, eid, placeholder_id)


def _update_luts_for_placeholders(
    entity_store: EntityStore,
    placeholders: pd.DataFrame,
) -> None:
    """Point LUT entries for placeholder synonyms to placeholder IDs."""
    for placeholder_id, row in placeholders.iterrows():
        lut = entity_store._get_lut(row["entity_type"])
        for synonym in row["synonyms"]:
            lut[synonym] = [placeholder_id]


def disambiguate_synonyms(entity_store: EntityStore) -> None:
    """Ensure every synonym is uniquely linked to one entity.

    When a synonym maps to multiple entity IDs, a placeholder entity is
    created that aggregates the ambiguous references. The original
    entities have the shared synonym removed and get back-pointers to
    the placeholder.
    """
    logger.info("Start disambiguating synonyms...")

    placeholder_rows = _collect_new_placeholders(entity_store)
    if not placeholder_rows:
        return

    placeholders = pd.DataFrame(placeholder_rows).set_index("foodatlas_id", drop=True)
    entity_store._entities = pd.concat([entity_store._entities, placeholders])

    _apply_placeholder_updates(entity_store, placeholders)
    _update_luts_for_placeholders(entity_store, placeholders)

    logger.info("Completed disambiguating synonyms!")
