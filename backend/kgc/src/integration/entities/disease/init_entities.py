"""Initialize disease entities from CTD data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from .constants import (
    CTD_ALTID_MAPPING,
    CTD_DISEASE_ID,
    ENTITY_TYPE,
    EXTERNAL_IDS,
    FA_ID,
)
from .loaders import filter_ctd_chemdis, load_ctd_chemdis, load_ctd_diseases

if TYPE_CHECKING:
    from ....models.settings import KGCSettings
    from ....stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def get_max_entity_id(entities: pd.DataFrame) -> int:
    """Extract the maximum numeric entity ID from a DataFrame."""
    id_ints = entities[FA_ID].str.extract(r"(\d+)").astype(int)
    return int(id_ints.max().iloc[0])


def parse_alt_disease_ids(row: pd.Series) -> dict:
    """Parse AltDiseaseIDs into structured external_ids dict."""
    alt_disease_ids = row["AltDiseaseIDs"]
    grouped: dict[str, list] = {}
    for item in alt_disease_ids:
        parts = str(item).split(":")
        prefix = parts[0]
        value = ":".join(parts[1:])
        mapped_key = CTD_ALTID_MAPPING.get(prefix, prefix)
        if mapped_key not in grouped:
            grouped[mapped_key] = []
        parsed = int(value) if value.isdigit() else value
        grouped[mapped_key].append(parsed)

    return {**row[EXTERNAL_IDS], **grouped}


def create_disease_entities(
    fa_entities: pd.DataFrame,
    ctd_diseases: pd.DataFrame,
    ctd_chemdis: pd.DataFrame,
    max_entity_id: int,
) -> pd.DataFrame:
    """Create disease entities from CTD data and append to existing entities."""
    ctd_diseases = ctd_diseases[
        ctd_diseases[CTD_DISEASE_ID].isin(ctd_chemdis[CTD_DISEASE_ID])
    ].reset_index(drop=True)

    ctd_diseases = ctd_diseases.copy()
    ctd_diseases[FA_ID] = [
        f"e{max_entity_id + i + 1}" for i in range(len(ctd_diseases))
    ]
    ctd_diseases[ENTITY_TYPE] = "disease"
    ctd_diseases["common_name"] = ctd_diseases["DiseaseName"].str.lower()
    ctd_diseases["scientific_name"] = ""
    ctd_diseases["synonyms"] = ctd_diseases["Synonyms"].apply(
        lambda x: [s.lower() for s in x] if isinstance(x, list) else []
    )
    ctd_diseases[EXTERNAL_IDS] = ctd_diseases[CTD_DISEASE_ID].apply(
        lambda x: {CTD_ALTID_MAPPING[x.split(":")[0]]: [":".join(x.split(":")[1:])]}
    )
    ctd_diseases[EXTERNAL_IDS] = ctd_diseases.apply(parse_alt_disease_ids, axis=1)

    return pd.concat([fa_entities, ctd_diseases], join="inner", ignore_index=True)


def append_diseases_from_ctd(entity_store: EntityStore, settings: KGCSettings) -> None:
    """Create disease entities from CTD and add to the entity store.

    Loads cleaned CTD data, filters to chemicals present in the KG,
    then creates disease entities for referenced diseases.
    """
    ctd_chemdis = load_ctd_chemdis(settings)
    ctd_diseases = load_ctd_diseases(settings)

    ctd_chemdis = filter_ctd_chemdis(ctd_chemdis, entity_store)

    entities_df = entity_store._entities.reset_index()
    max_eid = get_max_entity_id(entities_df)
    entities_df = create_disease_entities(
        entities_df, ctd_diseases, ctd_chemdis, max_eid
    )

    entity_store._entities = entities_df.set_index(FA_ID)
    entity_store._curr_eid = (
        entity_store._entities.index.str.slice(1).astype(int).max() + 1
    )

    logger.info("Imported disease entities from CTD.")
