"""Flavor entity and triplet creation."""

from __future__ import annotations

import pandas as pd

from ...models.relationship import RelationshipType


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
    """Create chemical→flavor triplets from flavor metadata.

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
