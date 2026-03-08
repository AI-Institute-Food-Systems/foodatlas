"""Initialize flavor entities from FlavorDB/HSDB data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from .loaders import filter_flavor_data, load_flavor_data

if TYPE_CHECKING:
    from ....models.settings import KGCSettings
    from ....stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def create_flavor_entities(
    flavor_metadata: pd.DataFrame,
    max_entity_id: int,
) -> pd.DataFrame:
    """Create new flavor entities from unique flavor descriptors."""
    descriptors = flavor_metadata["_flavor"].unique()
    return pd.DataFrame(
        {
            "foodatlas_id": [
                f"e{max_entity_id + i + 1}" for i in range(len(descriptors))
            ],
            "common_name": descriptors,
            "entity_type": "flavor",
            "scientific_name": "",
            "synonyms": [[] for _ in descriptors],
            "external_ids": [{} for _ in descriptors],
        }
    )


def append_flavors_from_flavordb(
    entity_store: EntityStore, settings: KGCSettings
) -> None:
    """Create flavor entities from FlavorDB/HSDB and add to entity store.

    Loads cleaned flavor metadata, filters to chemicals in the KG,
    creates unique flavor entities, and appends them.
    """
    data = load_flavor_data(settings)
    entities_df = entity_store._entities.reset_index()
    flavor_metadata = filter_flavor_data(data, entities_df)
    if flavor_metadata.empty:
        logger.info("No flavor metadata — no entities to import.")
        return

    max_eid = int(
        entities_df["foodatlas_id"].str.extract(r"(\d+)").astype(int).max().iloc[0]
    )
    flavor_ents = create_flavor_entities(flavor_metadata, max_eid)

    new_ents_indexed = flavor_ents.set_index("foodatlas_id")
    entity_store._entities = pd.concat([entity_store._entities, new_ents_indexed])
    entity_store._curr_eid = (
        entity_store._entities.index.str.slice(1).astype(int).max() + 1
    )

    logger.info("Imported %d flavor entities.", len(flavor_ents))
