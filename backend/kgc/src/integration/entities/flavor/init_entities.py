"""Initialize flavor entities from FlavorDB/HSDB data."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ....models.relationship import RelationshipType
from .loaders import build_flavor_metadata

if TYPE_CHECKING:
    from ....constructor.knowledge_graph import KnowledgeGraph
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)


def create_flavor_entities(
    flavor_metadata: pd.DataFrame,
    max_entity_id: int,
) -> pd.DataFrame:
    """Create new flavor entities from unique flavor descriptors.

    Args:
        flavor_metadata: Flavor metadata DataFrame with ``_flavor`` column.
        max_entity_id: Current maximum entity ID (integer).

    Returns:
        DataFrame of new flavor entities with generated IDs.
    """
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


def create_flavor_triplets(
    flavor_metadata: pd.DataFrame,
    entities: pd.DataFrame,
    max_triplet_id: int,
) -> pd.DataFrame:
    """Create chemical->flavor triplets from flavor metadata.

    Args:
        flavor_metadata: Flavor metadata with ``_pubchem_id`` and ``_flavor``.
        entities: Full entity DataFrame (must include chemical and flavor entities).
        max_triplet_id: Current maximum triplet ID (integer).

    Returns:
        DataFrame of new triplets with generated IDs.
    """
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


def append_flavors_from_flavordb(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Create flavor entities from FlavorDB/HSDB and add to the KG entity store.

    Builds flavor metadata, creates unique flavor entities, and appends
    them to the KG entity store.

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
        logger.info("No flavor metadata — no entities to import.")
        return

    max_eid = int(
        entities_df["foodatlas_id"].str.extract(r"(\d+)").astype(int).max().iloc[0]
    )
    flavor_ents = create_flavor_entities(flavor_metadata, max_eid)

    new_ents_indexed = flavor_ents.set_index("foodatlas_id")
    kg.entities._entities = pd.concat([kg.entities._entities, new_ents_indexed])
    kg.entities._curr_eid = (
        kg.entities._entities.index.str.slice(1).astype(int).max() + 1
    )

    logger.info("Imported %d flavor entities.", len(flavor_ents))
