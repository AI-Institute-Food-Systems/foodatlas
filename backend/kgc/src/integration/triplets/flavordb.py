"""Merge FlavorDB/HSDB flavor triplets and metadata into the knowledge graph."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ..entities.flavor.init_entities import create_flavor_triplets
from ..entities.flavor.loaders import build_flavor_metadata

if TYPE_CHECKING:
    from ...constructor.knowledge_graph import KnowledgeGraph
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def merge_flavordb_triplets(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Create flavor triplets and metadata from FlavorDB/HSDB data.

    Requires flavor entities to already be present in the KG (via
    ``initialization.flavor.init_entities.append_flavors_from_flavordb``).

    Args:
        kg: Loaded KnowledgeGraph instance with flavor entities.
        settings: KGCSettings with ``data_dir`` pointing to data root.
    """
    data_dir = Path(settings.data_dir)
    flavordb_path = data_dir / "FlavorDB" / "flavordb_scrape.json"
    hsdb_dir = data_dir / "HSDB"

    entities_df = kg.entities._entities.reset_index()
    flavor_metadata = build_flavor_metadata(flavordb_path, hsdb_dir, entities_df)
    if flavor_metadata.empty:
        logger.info("No flavor metadata — no triplets to create.")
        return

    max_tid = 0
    if not kg.triplets._triplets.empty:
        max_tid = int(kg.triplets._triplets.index.str.slice(1).astype(int).max())

    flavor_trips = create_flavor_triplets(flavor_metadata, entities_df, max_tid)

    if not flavor_trips.empty:
        trips_indexed = flavor_trips.set_index("foodatlas_id")
        kg.triplets._triplets = pd.concat([kg.triplets._triplets, trips_indexed])
        kg.triplets._curr_tid = (
            kg.triplets._triplets.index.str.slice(1).astype(int).max() + 1
        )

    flavor_metadata["entity_linking_method"] = "id_matching"
    flavor_metadata["_chemical_name"] = flavor_metadata["_pubchem_id"].apply(
        lambda x: f"PUBCHEM_COMPOUND:{x}"
    )
    flavor_metadata = flavor_metadata.rename(columns={"_flavor": "_flavor_name"})

    logger.info(
        "Merged %d flavor triplets, %d metadata rows.",
        len(flavor_trips),
        len(flavor_metadata),
    )
