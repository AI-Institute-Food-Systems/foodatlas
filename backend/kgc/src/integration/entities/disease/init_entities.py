"""Initialize disease entities from CTD data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.entity import DiseaseEntity
from .constants import (
    CTD_ALTID_MAPPING,
    CTD_DIRECTEVIDENCE,
    CTD_DISEASE_ID,
    EXTERNAL_IDS,
    FA_ID,
)
from .loaders import load_ctd_chemdis, load_ctd_diseases

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

    # Build external_ids using the existing parse_alt_disease_ids helper
    ctd_diseases = ctd_diseases.copy()
    ctd_diseases[EXTERNAL_IDS] = ctd_diseases[CTD_DISEASE_ID].apply(
        lambda x: {CTD_ALTID_MAPPING[x.split(":")[0]]: [":".join(x.split(":")[1:])]}
    )
    ctd_diseases[EXTERNAL_IDS] = ctd_diseases.apply(
        parse_alt_disease_ids, axis=1, result_type="reduce"
    )

    if ctd_diseases.empty:
        return fa_entities

    rows: list[dict] = []
    for i, (_, row) in enumerate(ctd_diseases.iterrows()):
        synonyms = (
            [s.lower() for s in row["Synonyms"]]
            if isinstance(row["Synonyms"], list)
            else []
        )
        entity = DiseaseEntity(
            foodatlas_id=f"e{max_entity_id + i + 1}",
            common_name=row["DiseaseName"].lower(),
            synonyms=synonyms,
            external_ids=row[EXTERNAL_IDS],
        )
        rows.append(entity.model_dump(by_alias=True))

    new_entities = pd.DataFrame(rows)
    return pd.concat([fa_entities, new_entities], join="inner", ignore_index=True)


def append_diseases_from_ctd(entity_store: EntityStore, settings: KGCSettings) -> None:
    """Create disease entities from CTD and add to the entity store.

    Loads cleaned CTD data, filters chemdis to direct-evidence rows,
    then creates disease entities for all referenced diseases.
    """
    ctd_chemdis = load_ctd_chemdis(settings)
    ctd_diseases = load_ctd_diseases(settings)

    ctd_chemdis = ctd_chemdis[ctd_chemdis[CTD_DIRECTEVIDENCE].notnull()].reset_index(
        drop=True
    )

    if ctd_chemdis.empty:
        logger.info("No direct-evidence CTD rows — skipping disease entities.")
        return

    entities_df = entity_store._entities.reset_index()
    max_eid = get_max_entity_id(entities_df)
    entities_df = create_disease_entities(
        entities_df, ctd_diseases, ctd_chemdis, max_eid
    )

    n_before = len(entity_store._entities)
    entity_store._entities = entities_df.set_index(FA_ID)
    entity_store._curr_eid = (
        entity_store._entities.index.str.slice(1).astype(int).max() + 1
    )
    n_added = len(entity_store._entities) - n_before

    logger.info("Added %d unique disease entities from CTD.", n_added)
