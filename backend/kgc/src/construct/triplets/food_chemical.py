"""Build food-chemical (CONTAINS) triplets from Phase 1 FDC edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.relationship import RelationshipType

if TYPE_CHECKING:
    from ...constructor.knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def merge_fdc_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create food-chemical CONTAINS triplets from FDC edges.

    Resolves FDC IDs directly to entity IDs via external_ids,
    bypassing name-based lookup to avoid creating duplicate entities.
    """
    fdc = sources.get("fdc")
    if fdc is None:
        return
    edges = fdc["edges"]
    contains = edges[edges["edge_type"] == "contains"]
    if contains.empty:
        logger.info("No FDC contains edges.")
        return

    nodes = fdc["nodes"]
    fdc2fa = _build_fdc_maps(kg.entities._entities)

    nutrient_units = _build_nutrient_unit_map(nodes)

    metadata_rows: list[dict] = []
    for _, edge in contains.iterrows():
        food_id = int(edge["head_native_id"].split(":")[-1])
        nutrient_id = int(edge["tail_native_id"].split(":")[-1])

        head_id = fdc2fa["food"].get(food_id)
        tail_id = fdc2fa["nutrient"].get(nutrient_id)
        if head_id is None or tail_id is None:
            continue

        attrs = edge["raw_attrs"] if isinstance(edge["raw_attrs"], dict) else {}
        amount = attrs.get("amount", 0.0)
        unit_name = nutrient_units.get(nutrient_id, "mg")
        conc_unit = f"{unit_name.lower()}/100g"

        metadata_rows.append(
            {
                "_food_name": f"FDC:{food_id}",
                "_chemical_name": f"FDC_NUTRIENT:{nutrient_id}",
                "_conc": f"{amount} {conc_unit}",
                "_food_part": "",
                "conc_value": amount,
                "conc_unit": conc_unit,
                "food_part": "",
                "food_processing": "",
                "source": "fdc",
                "reference": [
                    f"https://fdc.nal.usda.gov/fdc-app.html"
                    f"#/food-details/{food_id}/nutrients"
                ],
                "entity_linking_method": "id_matching",
                "quality_score": None,
                "_head_id": head_id,
                "_tail_id": tail_id,
            }
        )

    if not metadata_rows:
        logger.info("No FDC metadata to merge.")
        return

    metadata_df = pd.DataFrame(metadata_rows)

    stored = kg.metadata.create(metadata_df)

    stored["head_id"] = metadata_df["_head_id"].values
    stored["tail_id"] = metadata_df["_tail_id"].values
    stored["relationship_id"] = RelationshipType.CONTAINS
    triplets = kg.triplets.create(stored)

    logger.info("Merged %d FDC metadata, %d triplets.", len(stored), len(triplets))


def _build_fdc_maps(
    entities: pd.DataFrame,
) -> dict[str, dict[int, str]]:
    food_map: dict[int, str] = {}
    nutrient_map: dict[int, str] = {}
    for eid, row in entities.iterrows():
        ext = row["external_ids"]
        for fdc_id in ext.get("fdc", []):
            food_map[int(fdc_id)] = str(eid)
        for n_id in ext.get("fdc_nutrient", []):
            nutrient_map[int(n_id)] = str(eid)
    return {"food": food_map, "nutrient": nutrient_map}


def _build_nutrient_unit_map(nodes: pd.DataFrame) -> dict[int, str]:
    """Map nutrient ID → unit_name from FDC nodes."""
    nutrients = nodes[nodes["node_type"] == "nutrient"]
    result: dict[int, str] = {}
    for _, row in nutrients.iterrows():
        nid = int(row["native_id"].split(":")[-1])
        attrs = row.get("raw_attrs")
        if isinstance(attrs, dict):
            result[nid] = attrs.get("unit_name", "mg")
    return result
