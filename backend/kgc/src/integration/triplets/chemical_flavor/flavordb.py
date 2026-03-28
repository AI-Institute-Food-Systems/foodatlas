"""Apply FlavorDB/HSDB flavor descriptions to chemical entities."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from ...entities.flavor.loaders import filter_flavor_data, load_flavor_data

if TYPE_CHECKING:
    from ....constructor.knowledge_graph import KnowledgeGraph
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)


def apply_flavor_descriptions(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Add flavor descriptions from FlavorDB/HSDB directly to chemical entities.

    Instead of creating separate flavor entities and triplets, this stores
    flavor descriptors as a list on each chemical entity's
    ``_flavor_descriptions`` field.
    """
    data = load_flavor_data(settings)
    entities_df = kg.entities._entities.reset_index()
    flavor_data = filter_flavor_data(data, entities_df)
    if flavor_data.empty:
        logger.info("No flavor data — skipping flavor descriptions.")
        return

    chemicals = entities_df[entities_df["entity_type"] == "chemical"].copy()
    chemicals["pc_id"] = chemicals["external_ids"].apply(
        lambda x: int(x["pubchem_compound"][0]) if "pubchem_compound" in x else None
    )
    pc_to_faid = dict(zip(chemicals["pc_id"], chemicals["foodatlas_id"], strict=False))

    descs_by_entity: dict[str, set[str]] = defaultdict(set)
    for _, row in flavor_data.iterrows():
        fa_id = pc_to_faid.get(row["_pubchem_id"])
        if fa_id is None:
            continue
        descs_by_entity[fa_id].add(row["_flavor"])

    # Pre-create the column so pandas .at[] can assign list values.
    ents = kg.entities._entities
    if "_flavor_descriptions" not in ents.columns:
        ents["_flavor_descriptions"] = None

    n_updated = 0
    for fa_id, descs in descs_by_entity.items():
        ents.at[fa_id, "_flavor_descriptions"] = sorted(descs)
        n_updated += 1

    logger.info("Applied flavor descriptions to %d chemical entities.", n_updated)
