"""Build food-chemical (CONTAINS) triplets from Phase 1 FDC edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...constructor.knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def merge_fdc_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create food-chemical CONTAINS triplets from FDC edges."""
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

    rows: list[dict] = []
    for _, edge in contains.iterrows():
        food_native = edge["head_native_id"]
        nutrient_native = edge["tail_native_id"]
        food_id = int(food_native.split(":")[-1])
        nutrient_id = int(nutrient_native.split(":")[-1])

        if food_id not in fdc2fa["food"] or nutrient_id not in fdc2fa["nutrient"]:
            continue

        attrs = edge["raw_attrs"] if isinstance(edge["raw_attrs"], dict) else {}
        amount = attrs.get("amount", 0.0)

        nutrient_row = nodes[nodes["native_id"] == nutrient_native]
        unit_name = ""
        if not nutrient_row.empty:
            raw = nutrient_row.iloc[0].get("raw_attrs", {})
            if isinstance(raw, dict):
                unit_name = raw.get("unit_name", "")

        conc_unit = f"{unit_name.lower()}/100g" if unit_name else "mg/100g"

        rows.append(
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
            }
        )

    metadata = pd.DataFrame(rows)
    if metadata.empty:
        logger.info("No FDC metadata to merge.")
        return

    kg.add_triplets_from_metadata(metadata, relationship_type="contains")
    logger.info("Merged %d FDC metadata rows.", len(metadata))


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
