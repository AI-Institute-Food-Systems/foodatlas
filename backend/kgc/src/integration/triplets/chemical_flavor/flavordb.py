"""Merge FlavorDB/HSDB flavor triplets and metadata into the knowledge graph."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.relationship import RelationshipType
from ...entities.flavor.loaders import filter_flavor_data, load_flavor_data

if TYPE_CHECKING:
    from ....constructor.knowledge_graph import KnowledgeGraph
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)


def create_flavor_triplets(
    flavor_metadata: pd.DataFrame,
    entities: pd.DataFrame,
    max_triplet_id: int,
) -> pd.DataFrame:
    """Create chemical->flavor triplets from flavor metadata."""
    chemicals = entities[entities["entity_type"] == "chemical"].copy()
    chemicals["pc_id"] = chemicals["external_ids"].apply(
        lambda x: int(x["pubchem_compound"][0]) if "pubchem_compound" in x else None
    )
    flavors = entities[entities["entity_type"] == "flavor"]

    chem_pc_to_id = dict(
        zip(chemicals["pc_id"], chemicals["foodatlas_id"], strict=False)
    )
    flavor_name_to_id = dict(
        zip(flavors["common_name"], flavors["foodatlas_id"], strict=False)
    )

    rows: list[dict] = []
    for _, row in flavor_metadata.iterrows():
        head_id = chem_pc_to_id.get(row["_pubchem_id"])
        tail_id = flavor_name_to_id.get(row["_flavor"])
        if head_id is None or tail_id is None:
            continue
        rows.append(
            {
                "head_id": head_id,
                "relationship_id": RelationshipType.HAS_FLAVOR,
                "tail_id": tail_id,
                "metadata_ids": [row["foodatlas_id"]],
            }
        )

    triplets = pd.DataFrame(rows)
    if triplets.empty:
        return triplets

    triplets["foodatlas_id"] = [
        f"t{max_triplet_id + i + 1}" for i in range(len(triplets))
    ]
    return triplets


def merge_flavordb_triplets(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Create flavor triplets and metadata from FlavorDB/HSDB data.

    Requires flavor entities to already be present in the KG (via
    ``entities.flavor.init_entities.append_flavors_from_flavordb``).
    """
    data = load_flavor_data(settings)
    entities_df = kg.entities._entities.reset_index()
    flavor_data = filter_flavor_data(data, entities_df)
    if flavor_data.empty:
        logger.info("No flavor data - no triplets to create.")
        return

    flavor_data = flavor_data.copy()
    flavor_data["foodatlas_id"] = [f"mf{i + 1}" for i in range(len(flavor_data))]

    max_tid = 0
    if not kg.triplets._triplets.empty:
        max_tid = int(kg.triplets._triplets.index.str.slice(1).astype(int).max())

    flavor_trips = create_flavor_triplets(flavor_data, entities_df, max_tid)

    if not flavor_trips.empty:
        trips_indexed = flavor_trips.set_index("foodatlas_id")
        kg.triplets._triplets = pd.concat([kg.triplets._triplets, trips_indexed])
        kg.triplets._curr_tid = (
            kg.triplets._triplets.index.str.slice(1).astype(int).max() + 1
        )

    logger.info(
        "Merged %d flavor triplets, %d metadata rows.",
        len(flavor_trips),
        len(flavor_data),
    )
