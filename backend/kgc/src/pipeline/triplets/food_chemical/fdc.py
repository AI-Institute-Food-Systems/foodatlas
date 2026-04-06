"""Build food-chemical (CONTAINS) triplets from Phase 1 FDC edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.relationship import RelationshipType
from ...ie.conc_parser import convert_conc
from ..utils import explode_external_ids

if TYPE_CHECKING:
    from ...knowledge_graph import KnowledgeGraph

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
    contains = edges[edges["edge_type"] == "contains"].copy()
    if contains.empty:
        logger.info("No FDC contains edges.")
        return

    # Parse native IDs to integers for lookup.
    contains["_food_id"] = contains["head_native_id"].str.split(":").str[-1].astype(int)
    contains["_nutrient_id"] = (
        contains["tail_native_id"].str.split(":").str[-1].astype(int)
    )

    # Build lookup maps.
    food_lookup = explode_external_ids(kg.entities._entities, "fdc")
    nutrient_lookup = explode_external_ids(kg.entities._entities, "fdc_nutrient")
    if food_lookup.empty or nutrient_lookup.empty:
        logger.info("No FDC entity mappings.")
        return

    # Cast native_id to int for join.
    food_lookup["native_id"] = food_lookup["native_id"].astype(int)
    nutrient_lookup["native_id"] = nutrient_lookup["native_id"].astype(int)

    # Join head (food).
    df = contains.merge(
        food_lookup, left_on="_food_id", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_head_id", "candidates": "head_candidates"}
    )

    # Join tail (nutrient).
    df = df.merge(
        nutrient_lookup, left_on="_nutrient_id", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_tail_id", "candidates": "tail_candidates"}
    )

    if df.empty:
        logger.info("No FDC data to merge after resolution.")
        return

    # Extract concentration data — raw values first, then convert to mg/100g.
    nutrient_units = _build_nutrient_unit_map(fdc["nodes"])
    df["conc_value_raw"] = df["raw_attrs"].apply(
        lambda x: str(x.get("amount", "")) if isinstance(x, dict) else ""
    )
    df["conc_unit_raw"] = df["_nutrient_id"].map(
        lambda nid: f"{nutrient_units.get(nid, 'mg').lower()}/100g"
    )
    converted = df.apply(
        lambda r: convert_conc(r["conc_value_raw"], r["conc_unit_raw"]), axis=1
    )
    df["conc_value"] = converted.apply(lambda x: x[0] if x else None)
    df["conc_unit"] = converted.apply(lambda x: x[1] if x else "")

    # Build evidence + extraction columns.
    df["source_type"] = "fdc"
    df["reference"] = df["_food_id"].apply(
        lambda fid: json.dumps(
            {
                "url": f"https://fdc.nal.usda.gov/fdc-app.html#/food-details/{fid}/nutrients"
            }
        )
    )
    df["source"] = "fdc"
    df["head_name_raw"] = "FDC:" + df["_food_id"].astype(str)
    df["tail_name_raw"] = "FDC_NUTRIENT:" + df["_nutrient_id"].astype(str)

    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    attestations = kg.attestations.create(df)

    triplet_input = df[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id"])
    triplet_input.index = attestations.index
    triplet_input["relationship_id"] = RelationshipType.CONTAINS
    triplets = kg.triplets.create(triplet_input)

    logger.info(
        "Merged %d FDC attestations, %d triplets.", len(attestations), len(triplets)
    )


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
