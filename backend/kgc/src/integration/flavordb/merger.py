"""FlavorDB integration — orchestrate flavor data merging into KG."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .flavor_entities import create_flavor_entities, create_flavor_triplets
from .flavor_metadata import build_flavor_metadata

if TYPE_CHECKING:
    from ...constructor.knowledge_graph import KnowledgeGraph
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def merge_flavordb(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Merge FlavorDB and HSDB flavor data into the knowledge graph.

    Generates flavor metadata, creates flavor entities and triplets,
    and appends them to the KG stores.

    Args:
        kg: Loaded KnowledgeGraph instance.
        settings: KGCSettings with ``data_dir`` pointing to data root.
    """
    data_dir = Path(settings.data_dir)
    flavordb_path = data_dir / "FlavorDB" / "flavordb_scrape.json"
    hsdb_dir = data_dir / "HSDB"

    entities_df = kg.entities._entities.reset_index()

    flavor_metadata = build_flavor_metadata(flavordb_path, hsdb_dir, entities_df)
    if flavor_metadata.empty:
        logger.info("No flavor metadata to merge.")
        return

    max_eid = int(
        entities_df["foodatlas_id"].str.extract(r"(\d+)").astype(int).max().iloc[0]
    )
    flavor_ents = create_flavor_entities(flavor_metadata, max_eid)

    combined_entities = pd.concat([entities_df, flavor_ents], ignore_index=True)
    max_tid = 0
    if not kg.triplets._triplets.empty:
        max_tid = int(kg.triplets._triplets.index.str.slice(1).astype(int).max())

    flavor_trips = create_flavor_triplets(flavor_metadata, combined_entities, max_tid)

    new_ents_indexed = flavor_ents.set_index("foodatlas_id")
    kg.entities._entities = pd.concat([kg.entities._entities, new_ents_indexed])
    kg.entities._curr_eid = (
        kg.entities._entities.index.str.slice(1).astype(int).max() + 1
    )

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
        "Merged %d flavor entities, %d triplets, %d metadata rows.",
        len(flavor_ents),
        len(flavor_trips),
        len(flavor_metadata),
    )
