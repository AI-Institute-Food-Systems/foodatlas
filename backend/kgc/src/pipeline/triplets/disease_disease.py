"""Build disease ontology triplets (is_a) from Phase 1 CTD edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
from tqdm import tqdm

if TYPE_CHECKING:
    from ...stores.entity_store import EntityStore
    from ..knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_SOURCE = "ctd"
_REL_ID = "r2"


def merge_disease_ontology(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Generate is_a triplets from Phase 1 CTD disease hierarchy edges."""
    ctd = sources.get("ctd")
    if ctd is None:
        return

    edges = ctd["edges"]
    is_a_edges = edges[edges["edge_type"] == "is_a"]
    disease2fa = _build_disease_to_fa_map(kg.entities)

    rows: list[dict] = []
    for _, edge in tqdm(
        is_a_edges.iterrows(), total=len(is_a_edges), desc="disease is_a", leave=False
    ):
        head_ids = disease2fa.get(str(edge["head_native_id"]), [])
        tail_ids = disease2fa.get(str(edge["tail_native_id"]), [])
        if not head_ids or not tail_ids:
            continue
        ref = json.dumps({"source": _SOURCE, "edge_type": "is_a"})
        for head_id in head_ids:
            for tail_id in tail_ids:
                rows.append(
                    {
                        "source_type": _SOURCE,
                        "reference": ref,
                        "extractor": _SOURCE,
                        "head_name_raw": str(edge["head_native_id"]),
                        "tail_name_raw": str(edge["tail_native_id"]),
                        "head_candidates": head_ids,
                        "tail_candidates": tail_ids,
                        "_head_id": head_id,
                        "_tail_id": tail_id,
                    }
                )

    if not rows:
        logger.info("No CTD is_a edges to merge.")
        return

    df = pd.DataFrame(rows)
    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    extractions = kg.extractions.create(df)

    triplet_input = df[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = ["head_id", "tail_id"]
    triplet_input.index = extractions.index
    triplet_input["relationship_id"] = _REL_ID
    triplets = kg.triplets.create(triplet_input)

    logger.info("Created %d disease ontology triplets.", len(triplets))


def _build_disease_to_fa_map(entity_store: EntityStore) -> dict[str, list[str]]:
    disease2fa: dict[str, list[str]] = {}
    for faid, row in entity_store._entities.iterrows():
        if "ctd" not in row["external_ids"]:
            continue
        for ctd_id in row["external_ids"]["ctd"]:
            if ctd_id not in disease2fa:
                disease2fa[ctd_id] = []
            disease2fa[ctd_id].append(str(faid))
    return disease2fa
