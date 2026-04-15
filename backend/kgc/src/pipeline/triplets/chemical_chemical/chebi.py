"""Build chemical ontology triplets (is_a) from Phase 1 ChEBI edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ..utils import explode_external_ids

if TYPE_CHECKING:
    from ...knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_SOURCE = "chebi"
_REL_ID = "r2"


def merge_chemical_ontology(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Generate is_a triplets from Phase 1 ChEBI edges."""
    chebi = sources.get("chebi")
    if chebi is None:
        return

    edges = chebi["edges"]
    is_a = edges[edges["edge_type"] == "is_a"].copy()
    # ChEBI native IDs are integers in edges; cast to int for lookup.
    is_a["head_native_id"] = is_a["head_native_id"].astype(int).astype(str)
    is_a["tail_native_id"] = is_a["tail_native_id"].astype(int).astype(str)

    lookup = explode_external_ids(kg.entities._entities, "chebi")
    if lookup.empty:
        return

    # Natural is_a direction (matches FoodOn): head=child, tail=parent.
    # Phase-1 ChEBI edges already use this direction after the adapter
    # translates ChEBI's raw INIT=parent/FINAL=child convention.
    df = is_a.merge(
        lookup, left_on="head_native_id", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_head_id", "candidates": "head_candidates"}
    )

    df = df.merge(
        lookup, left_on="tail_native_id", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_tail_id", "candidates": "tail_candidates"}
    )

    if df.empty:
        logger.info("No ChEBI is_a edges to merge.")
        return

    ref = json.dumps({"source": _SOURCE, "edge_type": "is_a"})
    df["source_type"] = _SOURCE
    df["reference"] = ref
    df["source"] = _SOURCE
    df["head_name_raw"] = df["head_native_id"].astype(str)
    df["tail_name_raw"] = df["tail_native_id"].astype(str)

    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    attestations = kg.attestations.create(df)

    triplet_input = df[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id"])
    triplet_input.index = attestations.index
    triplet_input["relationship_id"] = _REL_ID
    triplets = kg.triplets.create(triplet_input)

    logger.info("Created %d chemical ontology attestations/triplets.", len(triplets))
