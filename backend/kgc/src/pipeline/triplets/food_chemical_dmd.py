"""Build food-chemical (CONTAINS) triplets from Phase 1 DMD edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.relationship import RelationshipType
from .chemical_disease import _explode_external_ids

if TYPE_CHECKING:
    from ..knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_COW_MILK_FOODON = "http://purl.obolibrary.org/obo/FOODON_02020891"


def _find_cow_milk_entity(entities: pd.DataFrame) -> str | None:
    """Find the cow milk entity ID by its FoodOn external ID."""
    for eid, row in entities.iterrows():
        for fon_id in row["external_ids"].get("foodon", []):
            if fon_id == _COW_MILK_FOODON:
                return str(eid)
    return None


def merge_dmd_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create food-chemical CONTAINS triplets from DMD edges."""
    dmd = sources.get("dmd")
    if dmd is None:
        return
    edges = dmd.get("edges", pd.DataFrame())
    if edges.empty:
        logger.info("No DMD edges.")
        return

    contains = edges[edges["edge_type"] == "contains"].copy()
    if contains.empty:
        logger.info("No DMD contains edges.")
        return

    milk_fa_id = _find_cow_milk_entity(kg.entities._entities)
    if not milk_fa_id:
        logger.warning("Cow milk entity (FOODON_02020891) not found; skipping DMD.")
        return

    mol_lookup = _explode_external_ids(kg.entities._entities, "dmd")
    if mol_lookup.empty:
        logger.info("No DMD entity mappings.")
        return

    df = contains.merge(
        mol_lookup,
        left_on="tail_native_id",
        right_on="native_id",
        how="inner",
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_tail_id", "candidates": "tail_candidates"}
    )

    if df.empty:
        logger.info("No DMD data to merge after resolution.")
        return

    df["_head_id"] = milk_fa_id
    df["head_candidates"] = [[milk_fa_id]] * len(df)

    df["conc_value"] = df["raw_attrs"].apply(
        lambda x: x.get("conc_value") if isinstance(x, dict) else None
    )
    df["conc_unit"] = df["raw_attrs"].apply(
        lambda x: x.get("conc_unit", "") if isinstance(x, dict) else ""
    )

    df["source_type"] = "dmd"
    df["reference"] = df["raw_attrs"].apply(
        lambda x: json.dumps(
            {
                "url": "https://dairymolecules.com/database",
                "dmd_concentration_id": x.get("dmd_concentration_id", "")
                if isinstance(x, dict)
                else "",
            }
        )
    )
    df["source"] = "dmd"
    df["head_name_raw"] = "milk"
    df["tail_name_raw"] = "DMD:" + df["tail_native_id"]

    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    attestations = kg.attestations.create(df)

    triplet_input = df[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id"])
    triplet_input.index = attestations.index
    triplet_input["relationship_id"] = RelationshipType.CONTAINS
    triplets = kg.triplets.create(triplet_input)

    logger.info(
        "Merged %d DMD attestations, %d triplets.", len(attestations), len(triplets)
    )
