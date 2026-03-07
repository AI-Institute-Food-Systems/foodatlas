"""Disease entity creation from CTD data."""

from __future__ import annotations

import logging

import pandas as pd

from .constants import (
    CTD_ALTID_MAPPING,
    CTD_DISEASE_ID,
    ENTITY_TYPE,
    EXTERNAL_IDS,
    FA_ID,
)

logger = logging.getLogger(__name__)


def get_max_entity_id(entities: pd.DataFrame) -> int:
    """Extract the maximum numeric entity ID from a DataFrame.

    Args:
        entities: DataFrame with ``foodatlas_id`` column (e.g. ``"e42"``).

    Returns:
        Maximum integer portion of the IDs.
    """
    id_ints = entities[FA_ID].str.extract(r"(\d+)").astype(int)
    return int(id_ints.max().iloc[0])


def parse_alt_disease_ids(row: pd.Series) -> dict:
    """Parse AltDiseaseIDs into structured external_ids dict.

    Merges alternative disease identifiers (MESH, OMIM, DO) from the
    ``AltDiseaseIDs`` column with the row's existing ``external_ids``.

    Args:
        row: A row from the CTD diseases DataFrame.

    Returns:
        Merged external_ids dict with mapped keys.
    """
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
    """Create disease entities from CTD data and append to existing entities.

    Filters CTD diseases to only those referenced in ``ctd_chemdis``,
    assigns new entity IDs starting after ``max_entity_id``.

    Args:
        fa_entities: Existing FoodAtlas entities DataFrame.
        ctd_diseases: Full CTD diseases DataFrame.
        ctd_chemdis: Filtered CTD chemical-disease DataFrame.
        max_entity_id: Current maximum entity ID (integer).

    Returns:
        Combined DataFrame with original and new disease entities.
    """
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
