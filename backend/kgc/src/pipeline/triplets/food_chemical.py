"""Build food-chemical (CONTAINS) triplets from Phase 1 FDC edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.relationship import RelationshipType

if TYPE_CHECKING:
    from ..knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def merge_fdc_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create food-chemical CONTAINS triplets from FDC edges.

    Creates evidence (FDC URL) and extraction (parsed concentration)
    records, then links them to triplets.
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

    rows: list[dict] = []
    for _, edge in contains.iterrows():
        food_id = int(edge["head_native_id"].split(":")[-1])
        nutrient_id = int(edge["tail_native_id"].split(":")[-1])

        head_ids = fdc2fa["food"].get(food_id, [])
        tail_ids = fdc2fa["nutrient"].get(nutrient_id, [])
        if not head_ids or not tail_ids:
            continue

        attrs = edge["raw_attrs"] if isinstance(edge["raw_attrs"], dict) else {}
        amount = attrs.get("amount", 0.0)
        unit_name = nutrient_units.get(nutrient_id, "mg")
        conc_unit = f"{unit_name.lower()}/100g"

        ref = json.dumps(
            {
                "url": f"https://fdc.nal.usda.gov/fdc-app.html#/food-details/{food_id}/nutrients"
            }
        )
        for head_id in head_ids:
            for tail_id in tail_ids:
                rows.append(
                    {
                        "source_type": "fdc",
                        "reference": ref,
                        "extractor": "fdc",
                        "head_name_raw": f"FDC:{food_id}",
                        "tail_name_raw": f"FDC_NUTRIENT:{nutrient_id}",
                        "conc_value": amount,
                        "conc_unit": conc_unit,
                        "food_part": "",
                        "food_processing": "",
                        "quality_score": None,
                        "_head_id": head_id,
                        "_tail_id": tail_id,
                    }
                )

    if not rows:
        logger.info("No FDC data to merge.")
        return

    df = pd.DataFrame(rows)

    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    extractions = kg.extractions.create(df)

    triplet_input = df[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = ["head_id", "tail_id"]
    triplet_input.index = extractions.index
    triplet_input["relationship_id"] = RelationshipType.CONTAINS
    triplets = kg.triplets.create(triplet_input)

    logger.info(
        "Merged %d FDC extractions, %d triplets.", len(extractions), len(triplets)
    )


def _build_fdc_maps(
    entities: pd.DataFrame,
) -> dict[str, dict[int, list[str]]]:
    food_map: dict[int, list[str]] = {}
    nutrient_map: dict[int, list[str]] = {}
    for eid, row in entities.iterrows():
        ext = row["external_ids"]
        for fdc_id in ext.get("fdc", []):
            key = int(fdc_id)
            if key not in food_map:
                food_map[key] = []
            food_map[key].append(str(eid))
        for n_id in ext.get("fdc_nutrient", []):
            key = int(n_id)
            if key not in nutrient_map:
                nutrient_map[key] = []
            nutrient_map[key].append(str(eid))
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
