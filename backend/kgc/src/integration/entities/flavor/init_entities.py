"""Initialize flavor entities from FlavorDB/HSDB data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.entity import FlavorEntity
from .loaders import load_flavor_data

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
    rows = [
        FlavorEntity(
            foodatlas_id=f"e{max_entity_id + i + 1}",
            common_name=desc,
        ).model_dump(by_alias=True)
        for i, desc in enumerate(descriptors)
    ]
    return pd.DataFrame(rows)


def append_flavors_from_flavordb(
    entity_store: EntityStore, settings: KGCSettings
) -> None:
    """Create flavor entities from all FlavorDB/HSDB descriptors.

    Loads cleaned flavor data and creates unique flavor entities.
    Chemical filtering happens later during triplet creation.
    """
    data = load_flavor_data(settings)
    if data.empty:
        logger.info("No flavor data — no entities to import.")
        return

    entities_df = entity_store._entities.reset_index()
    max_eid = int(
        entities_df["foodatlas_id"].str.extract(r"(\d+)").astype(int).max().iloc[0]
    )
    flavor_ents = create_flavor_entities(data, max_eid)

    new_ents_indexed = flavor_ents.set_index("foodatlas_id")
    entity_store._entities = pd.concat([entity_store._entities, new_ents_indexed])
    entity_store._curr_eid = (
        entity_store._entities.index.str.slice(1).astype(int).max() + 1
    )

    logger.info("Added %d unique flavor entities from FlavorDB.", len(flavor_ents))
