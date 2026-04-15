"""Build chemical ontology triplets (is_a) from Phase 1 CDNO edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ..utils import explode_external_ids

if TYPE_CHECKING:
    from ...knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_SOURCE = "cdno"
_REL_ID = "r2"


def merge_chemical_ontology_cdno(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Generate is_a triplets from Phase 1 CDNO edges.

    CDNO classifies chemicals into nutrient categories (vitamin, amino acid,
    lipid, etc.). Most CDNO leaves and intermediates have ChEBI equivalents and
    therefore share a foodatlas_id with their ChEBI counterpart after xref
    resolution — so these triplets express "is_a" relationships between the
    merged chemical entities without introducing duplicates.
    """
    cdno = sources.get("cdno")
    if cdno is None:
        return

    edges = cdno["edges"]
    is_a = edges[edges["edge_type"] == "is_a"].copy()
    if is_a.empty:
        logger.info("No CDNO is_a edges to merge.")
        return

    lookup = explode_external_ids(kg.entities._entities, _SOURCE)
    if lookup.empty:
        logger.info("No CDNO entities resolved; skipping CDNO ontology merge.")
        return

    # Natural is_a direction: head=child, tail=parent.
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

    # Drop self-loops that appear after xref-based entity merging (when a CDNO
    # child and its CDNO parent both map to the same ChEBI entity).
    df = df[df["_head_id"] != df["_tail_id"]]

    if df.empty:
        logger.info("No CDNO is_a edges to merge after resolution.")
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

    logger.info("Created %d CDNO chemical ontology triplets.", len(triplets))
