"""Initialize chemical entities from ChEBI, CDNO, and FDC nutrients."""

import logging

import pandas as pd

from ....models.settings import KGCSettings
from ....stores.entity_store import EntityStore
from .loaders import (
    load_cdno,
    load_fdc_nutrient,
    load_mapper_chebi_id_to_names,
    load_mapper_name_to_chebi_id,
)

logger = logging.getLogger(__name__)


def _add_to_lut(row: pd.Series, lut_chemical: dict[str, list[str]]) -> None:
    if row["entity_type"] != "chemical":
        return
    for syn in row["synonyms"]:
        if syn in lut_chemical:
            msg = f"Duplicate synonym: {syn}"
            raise ValueError(msg)
        lut_chemical[syn] = [row.name]


def append_chemicals_from_chebi(
    entity_store: EntityStore,
    settings: KGCSettings,
) -> None:
    """Create chemical entities from ChEBI with placeholder handling."""
    logger.info("Initializing chemical entities from ChEBI.")

    chebi2name = load_mapper_chebi_id_to_names(settings)
    name2chebi = load_mapper_name_to_chebi_id(settings)

    # Identify placeholder entities (ambiguous names mapping to >1 ChEBI ID).
    name2chebi_ph = name2chebi[name2chebi["CHEBI_ID"].apply(len) > 1].copy()
    name2chebi_ph["CHEBI_ID"] = name2chebi_ph["CHEBI_ID"].apply(sorted)
    name2chebi_ph["_chebi_id_str"] = name2chebi_ph["CHEBI_ID"].apply(str)
    phs = (
        name2chebi_ph.groupby("_chebi_id_str")
        .apply(
            lambda x: pd.Series(
                {"CHEBI_ID": x["CHEBI_ID"].values[0], "NAME": x["NAME"].tolist()}
            )
        )
        .reset_index(drop=True)
    )

    # Remove placeholder names from unique entity synonyms.
    chebi2name = chebi2name.set_index("CHEBI_ID")
    for _, row in phs.iterrows():
        for chebi_id in row["CHEBI_ID"]:
            for name in row["NAME"]:
                chebi2name.at[chebi_id, "NAME"].remove(name)
    chebi2name = chebi2name.reset_index()

    _add_unique_chebi_entities(entity_store, chebi2name)
    _add_placeholder_entities(entity_store, phs)


def _add_unique_chebi_entities(
    entity_store: EntityStore,
    chebi2name: pd.DataFrame,
) -> None:
    """Add unique ChEBI entities to the store."""
    entities_new_rows = []
    for _, row in chebi2name.iterrows():
        entities_new_rows.append(
            {
                "foodatlas_id": f"e{entity_store._curr_eid}",
                "entity_type": "chemical",
                "common_name": row["NAME"][0],
                "scientific_name": None,
                "synonyms": row["NAME"],
                "external_ids": {"chebi": [row["CHEBI_ID"]]},
            }
        )
        entity_store._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index("foodatlas_id")
    entity_store._entities = pd.concat([entity_store._entities, entities_new])
    entity_store._lut_chemical = {}
    for _, row in entities_new.iterrows():
        _add_to_lut(row, entity_store._lut_chemical)

    logger.info("Added %d unique chemical entities from ChEBI.", len(entities_new))


def _add_placeholder_entities(
    entity_store: EntityStore,
    phs: pd.DataFrame,
) -> None:
    """Add placeholder entities for ambiguous ChEBI names."""
    chebi2fa: dict[int, str] = {}
    for entity_id, row in entity_store._entities.iterrows():
        if row["entity_type"] == "chemical" and "chebi" in row["external_ids"]:
            chebi2fa[row["external_ids"]["chebi"][0]] = str(entity_id)

    entities_new_rows = []
    for _, row in phs.iterrows():
        entities_new_rows.append(
            {
                "foodatlas_id": f"e{entity_store._curr_eid}",
                "entity_type": "chemical",
                "common_name": row["NAME"][0],
                "scientific_name": None,
                "synonyms": row["NAME"],
                "external_ids": {
                    "_placeholder_to": [chebi2fa[cid] for cid in row["CHEBI_ID"]],
                },
            }
        )
        entity_store._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index("foodatlas_id")
    entity_store._entities = pd.concat([entity_store._entities, entities_new])
    for _, row in entities_new.iterrows():
        _add_to_lut(row, entity_store._lut_chemical)

    logger.info("Added %d placeholder chemical entities.", len(entities_new))


def append_chemicals_from_cdno(
    entity_store: EntityStore,
    settings: KGCSettings,
) -> None:
    """Add CDNO entities, linking to existing ChEBI entities where possible."""
    logger.info("Initializing chemical entities from CDNO.")
    cdno = load_cdno(settings)

    chebi2fa: dict[int, str] = {}
    for entity_id, row in entity_store._entities.iterrows():
        if row["entity_type"] == "chemical" and "chebi" in row["external_ids"]:
            chebi2fa[row["external_ids"]["chebi"][0]] = str(entity_id)

    n_linked = 0
    entities_not_added = []
    for _, row in cdno.iterrows():
        chebi_id = row["chebi_id"]
        if chebi_id in chebi2fa:
            fa_id = chebi2fa[chebi_id]
            entity_store._entities.at[fa_id, "external_ids"]["cdno"] = [row["cdno_id"]]
            if row["fdc_nutrient_ids"]:
                entity_store._entities.at[fa_id, "external_ids"]["fdc_nutrient"] = row[
                    "fdc_nutrient_ids"
                ]
            n_linked += 1
        else:
            entities_not_added.append(row)

    logger.info("Linked %d CDNO IDs to existing entities.", n_linked)
    _add_new_cdno_entities(entity_store, pd.DataFrame(entities_not_added))


def _add_new_cdno_entities(
    entity_store: EntityStore, entities_not_added: pd.DataFrame
) -> None:
    """Create new entities for CDNO entries not linked to ChEBI."""
    entities_new_rows = []
    for _, row in entities_not_added.iterrows():
        external_ids: dict[str, list[object]] = {}
        if pd.notna(row["chebi_id"]):
            external_ids["chebi"] = [row["chebi_id"]]
        external_ids["cdno"] = [row["cdno_id"]]
        external_ids["fdc_nutrient"] = row["fdc_nutrient_ids"]

        entities_new_rows.append(
            {
                "foodatlas_id": f"e{entity_store._curr_eid}",
                "entity_type": "chemical",
                "common_name": row["label"],
                "scientific_name": None,
                "synonyms": [row["label"]],
                "external_ids": external_ids,
            }
        )
        entity_store._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index("foodatlas_id")
    entity_store._entities = pd.concat([entity_store._entities, entities_new])
    for _, row in entities_new.iterrows():
        _add_to_lut(row, entity_store._lut_chemical)

    logger.info("Added %d unique chemical entities from CDNO.", len(entities_new))


def append_chemicals_from_fdc(
    entity_store: EntityStore,
    settings: KGCSettings,
) -> None:
    """Add FDC nutrient entities not already linked via CDNO."""
    logger.info("Initializing chemical entities from FDC.")

    fdc2fa: dict[int, str] = {}
    for entity_id, row in entity_store._entities.iterrows():
        if row["entity_type"] != "chemical":
            continue
        if "fdc_nutrient" in row["external_ids"]:
            for fdc_id in row["external_ids"]["fdc_nutrient"]:
                if fdc_id in fdc2fa:
                    msg = f"Duplicate FDC ID: {fdc_id}"
                    raise ValueError(msg)
                fdc2fa[fdc_id] = str(entity_id)

    fdc = load_fdc_nutrient(settings)
    n_linked = 0
    entities_not_added = []
    for fdc_id, row in fdc.iterrows():
        if fdc_id in fdc2fa:
            n_linked += 1
        elif row["name"] in entity_store._lut_chemical:
            fa_id = entity_store._lut_chemical[row["name"]][0]
            entity_store._entities.at[fa_id, "external_ids"]["fdc_nutrient"] = [fdc_id]
            n_linked += 1
        else:
            entities_not_added.append(row)

    logger.info("Linked %d FDC Nutrient IDs to existing entities.", n_linked)
    _add_new_fdc_entities(entity_store, pd.DataFrame(entities_not_added))


def _add_new_fdc_entities(
    entity_store: EntityStore, entities_not_added: pd.DataFrame
) -> None:
    """Create new entities for unlinked FDC nutrients."""
    entities_new_rows = []
    for fdc_id, row in entities_not_added.iterrows():
        entities_new_rows.append(
            {
                "foodatlas_id": f"e{entity_store._curr_eid}",
                "entity_type": "chemical",
                "common_name": row["name"],
                "scientific_name": None,
                "synonyms": [row["name"]],
                "external_ids": {"fdc_nutrient": [fdc_id]},
            }
        )
        entity_store._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index("foodatlas_id")
    entity_store._entities = pd.concat([entity_store._entities, entities_new])
    for _, row in entities_new.iterrows():
        _add_to_lut(row, entity_store._lut_chemical)

    logger.info("Added %d unique chemical entities from FDC.", len(entities_new))
