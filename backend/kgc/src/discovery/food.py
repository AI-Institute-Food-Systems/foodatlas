"""Food entity creation from NCBI Taxonomy and synonym grouping."""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import TYPE_CHECKING

import pandas as pd
from inflection import pluralize, singularize

from ..models.entity import FoodEntity
from ..stores.schema import ENTITY_COLUMNS
from ..utils import get_lookup_key_by_id, merge_sets
from .query import query_ncbi_taxonomy

if TYPE_CHECKING:
    from ..stores.entity_store import EntityStore

logger = logging.getLogger(__name__)

COLUMNS = ENTITY_COLUMNS


def _group_synonyms(synonyms_groups: list[list[str]]) -> list[list[str]]:
    """Merge overlapping synonym groups into disjoint sets."""
    logger.info("Start grouping synonyms...")
    sets = [set(synonyms) for synonyms in synonyms_groups]
    merged = merge_sets(sets)
    result = [sorted(s) for s in merged]
    logger.info("Completed grouping synonyms!")
    return result


def _parse_ncbi_names(row: pd.Series) -> pd.Series:
    """Parse NCBI taxonomy record into entity fields."""
    scientific_name = row["ScientificName"]
    other_names = row["OtherNames"]

    synonyms_scientific = [scientific_name]
    synonyms_common: list[str] = []
    synonyms_others: list[str] = []

    if other_names is not None:
        synonyms_scientific += other_names["Synonym"]
        synonyms_scientific += other_names["EquivalentName"]
        if other_names["Name"]:
            for name in other_names["Name"]:
                if name["ClassCDE"] in ("misspelling", "authority"):
                    synonyms_scientific.append(name["DispName"])

        synonyms_common += other_names["CommonName"]
        if "GenbankCommonName" in other_names:
            synonyms_common.append(other_names["GenbankCommonName"])
        if "BlastName" in other_names:
            synonyms_common.append(other_names["BlastName"])

        synonyms_others += other_names["Includes"]

    row["scientific_name"] = scientific_name.strip().lower()

    if synonyms_common:
        synonyms_common_sp: list[str] = []
        for name in synonyms_common:
            synonyms_common_sp += [singularize(name), pluralize(name)]
        synonyms_common += synonyms_common_sp
        row["common_name"] = min(synonyms_common, key=len).strip().lower()
    else:
        row["common_name"] = row["scientific_name"]

    if len(scientific_name.split(" ")) > 1:
        synonyms_scientific_abbr: list[str] = []
        for name in synonyms_scientific:
            terms = name.split(" ")
            terms[0] = terms[0][0] + "."
            synonyms_scientific_abbr.append(" ".join(terms))
        synonyms_scientific += synonyms_scientific_abbr

    synonyms = synonyms_common + synonyms_others + synonyms_scientific
    synonyms = list(OrderedDict.fromkeys(synonyms).keys())
    synonyms = [x.strip().lower() for x in synonyms]
    row["synonyms"] = [
        *synonyms,
        get_lookup_key_by_id("ncbi_taxon_id", row["TaxId"]),
    ]
    return row


def _create_from_ncbi_taxonomy(
    store: EntityStore,
    records: pd.DataFrame,
) -> None:
    """Create food entities that have NCBI Taxonomy IDs."""
    logger.info("Start creating entities with NCBI Taxonomy IDs...")

    if records.empty:
        return

    entities_new = records.copy()
    entities_new[COLUMNS] = None
    entities_new = entities_new.apply(_parse_ncbi_names, axis=1)
    entities_new = entities_new.rename(columns={"TaxId": "ncbi_taxon_id"})
    entities_new["external_ids"] = entities_new["ncbi_taxon_id"].apply(
        lambda x: {"ncbi_taxon_id": x}
    )

    entities_new["foodatlas_id"] = [
        f"e{i}" for i in range(store._curr_eid, store._curr_eid + len(entities_new))
    ]
    store._curr_eid += len(entities_new)

    entities_new["entity_type"] = "food"
    entities_new = entities_new[COLUMNS].set_index("foodatlas_id")
    store._entities = pd.concat([store._entities, entities_new])
    store.update_lut(entities_new)

    logger.info("Completed!")


def _create_from_synonym_groups(
    store: EntityStore,
    synonym_groups: list[list[str]],
) -> None:
    """Create food entities without NCBI Taxonomy IDs."""
    logger.info("Start creating entities without NCBI Taxonomy IDs...")

    rows: list[dict] = []
    for synonyms in synonym_groups:
        found = any(store.get_entity_ids("food", name) for name in synonyms)
        if not found:
            entity = FoodEntity(
                foodatlas_id=f"e{store._curr_eid}",
                common_name=min(synonyms, key=len),
                synonyms=synonyms,
            )
            rows.append(entity.model_dump(by_alias=True))
            store._curr_eid += 1

    if rows:
        entities_new = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, entities_new])
        store.update_lut(entities_new)

    logger.info("Completed!")


def create_food_entities(
    store: EntityStore,
    entity_names_new: list[str],
) -> None:
    """Create food entities — first via NCBI Taxonomy, then by synonym groups.

    Requires ``query_ncbi_taxonomy`` to be available at
    ``src.query.query_ncbi_taxonomy`` (implemented in a later story).
    """
    entity_synonyms = [
        list({name, singularize(name), pluralize(name)}) for name in entity_names_new
    ]
    entity_names_all = list({name for names in entity_synonyms for name in names})

    records = query_ncbi_taxonomy(
        entity_names_all,
        store.path_kg,
        store.path_cache_dir,
    )
    _create_from_ncbi_taxonomy(store, records)

    entity_synonyms_grouped = _group_synonyms(entity_synonyms)
    _create_from_synonym_groups(store, entity_synonyms_grouped)

    for synonyms in entity_synonyms_grouped:
        found = False
        for name in synonyms:
            eids = store.get_entity_ids("food", name)
            for eid in eids:
                store.update_entity_synonyms(eid, synonyms)
                found = True
        if not found:
            msg = f"Entity not found for synonyms: {synonyms}"
            raise ValueError(msg)
