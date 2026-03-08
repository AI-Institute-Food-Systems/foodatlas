"""Chemical entity creation from PubChem Compound records."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ..models.entity import ChemicalEntity
from ..utils import get_lookup_key_by_id
from .query import query_pubchem_compound

if TYPE_CHECKING:
    from ..stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def _parse_pubchem_names(row: pd.Series) -> pd.Series:
    """Parse PubChem record into entity fields."""
    row["scientific_name"] = (
        row["IUPACName"].strip().lower() if not pd.isna(row["IUPACName"]) else None
    )

    row["synonyms"] = [s.strip().lower() for s in row["SynonymList"]]
    if row["scientific_name"] and row["scientific_name"] not in row["synonyms"]:
        row["synonyms"].append(row["scientific_name"])
    row["synonyms"].append(get_lookup_key_by_id("pubchem_cid", row["CID"]))
    row["common_name"] = row["synonyms"][0]

    return row


def _create_from_pubchem_compound(
    store: EntityStore,
    records: pd.DataFrame,
) -> None:
    """Create chemical entities that have PubChem CIDs."""
    logger.info("Start creating entities with PubChem CIDs...")

    parsed = records.apply(_parse_pubchem_names, axis=1)

    rows: list[dict] = []
    for _, row in parsed.iterrows():
        lookup_key = get_lookup_key_by_id("pubchem_cid", row["CID"])
        if lookup_key in store._lut_chemical:
            eid = store._lut_chemical[lookup_key][0]
            for synonym in row["synonyms"]:
                if synonym not in store._entities.at[eid, "synonyms"]:
                    store._entities.at[eid, "synonyms"] += [synonym]
                    if synonym not in store._lut_chemical:
                        store._lut_chemical[synonym] = []
                    if eid not in store._lut_chemical[synonym]:
                        store._lut_chemical[synonym].append(eid)
            continue

        entity = ChemicalEntity(
            foodatlas_id=f"e{store._curr_eid}",
            common_name=row["common_name"],
            scientific_name=row["scientific_name"] or "",
            synonyms=row["synonyms"],
            external_ids={"pubchem_cid": [row["CID"]]},
        )
        rows.append(entity.model_dump(by_alias=True))
        store._curr_eid += 1

    if rows:
        entities_new = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, entities_new])
        store.update_lut(entities_new)

    logger.info("Completed!")


def _create_from_names(
    store: EntityStore,
    names: list[str],
) -> None:
    """Create chemical entities without PubChem CIDs."""
    logger.info("Start creating entities without PubChem CIDs...")

    rows: list[dict] = []
    for name in names:
        if not store.get_entity_ids("chemical", name):
            entity = ChemicalEntity(
                foodatlas_id=f"e{store._curr_eid}",
                common_name=name,
                synonyms=[name],
            )
            rows.append(entity.model_dump(by_alias=True))
            store._curr_eid += 1

    if rows:
        entities_new = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, entities_new])
        store.update_lut(entities_new)

    logger.info("Completed!")


def create_chemical_entities(
    store: EntityStore,
    entity_names_new: list[str],
) -> None:
    """Create chemical entities — first via PubChem, then by name.

    Requires ``query_pubchem_compound`` to be available at
    ``src.query.query_pubchem_compound`` (implemented in a later story).
    """
    records = query_pubchem_compound(
        entity_names_new,
        store.path_kg,
        store.path_cache_dir,
    )
    if not records.empty:
        _create_from_pubchem_compound(store, records)
    _create_from_names(store, entity_names_new)
