"""Apply FlavorDB/HSDB flavor descriptions to chemical entities."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from ...models.attributes import ChemicalAttributes
from .utils import read_attributes, write_attributes

if TYPE_CHECKING:
    import pandas as pd

    from ..knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def apply_flavor_descriptions(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Add flavor descriptors from Phase 1 FlavorDB output to chemical entities.

    Writes into ``attributes.flavor_descriptors`` via :class:`ChemicalAttributes`.
    """
    flavordb = sources.get("flavordb")
    if flavordb is None:
        return
    nodes = flavordb["nodes"]
    if nodes.empty:
        logger.info("No flavor data — skipping.")
        return

    entities = kg.entities._entities.reset_index()
    chemicals = entities[entities["entity_type"] == "chemical"].copy()
    chemicals["pc_id"] = chemicals["external_ids"].apply(
        lambda x: int(x["pubchem_compound"][0]) if "pubchem_compound" in x else None
    )
    pc_to_faid = dict(zip(chemicals["pc_id"], chemicals["foodatlas_id"], strict=False))

    descs_by_entity: dict[str, set[str]] = defaultdict(set)
    for _, row in nodes.iterrows():
        pc_id = int(row["native_id"])
        fa_id = pc_to_faid.get(pc_id)
        if fa_id is None:
            continue
        raw = row.get("raw_attrs", {})
        if isinstance(raw, dict):
            for flavor in raw.get("flavors", []):
                descs_by_entity[fa_id].add(flavor)

    ents = kg.entities._entities
    for fa_id, descriptors in descs_by_entity.items():
        attrs = read_attributes(ents, fa_id, ChemicalAttributes)
        attrs.flavor_descriptors = sorted(descriptors)
        write_attributes(ents, fa_id, attrs)

    logger.info(
        "Applied flavor descriptions to %d chemical entities.", len(descs_by_entity)
    )
