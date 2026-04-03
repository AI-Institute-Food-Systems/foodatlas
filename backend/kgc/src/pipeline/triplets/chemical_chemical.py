"""Build chemical ontology triplets (is_a) from Phase 1 ChEBI edges."""

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
    is_a_edges = edges[edges["edge_type"] == "is_a"]
    chebi2fa = _build_chebi_to_fa_map(kg.entities)

    rows: list[dict] = []
    for _, edge in tqdm(
        is_a_edges.iterrows(), total=len(is_a_edges), desc="chem is_a", leave=False
    ):
        head_key = int(edge["head_native_id"])
        tail_key = int(edge["tail_native_id"])
        # ChEBI is_a semantics: head is_a tail → reversed in KG
        head_ids = chebi2fa.get(tail_key, [])
        tail_ids = chebi2fa.get(head_key, [])
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
                        "head_name_raw": str(tail_key),
                        "tail_name_raw": str(head_key),
                        "head_candidates": head_ids,
                        "tail_candidates": tail_ids,
                        "_head_id": head_id,
                        "_tail_id": tail_id,
                    }
                )

    if not rows:
        logger.info("No ChEBI is_a edges to merge.")
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

    logger.info("Created %d chemical ontology triplets.", len(triplets))


def _build_chebi_to_fa_map(entity_store: EntityStore) -> dict[int, list[str]]:
    chebi2fa: dict[int, list[str]] = {}
    for faid, row in entity_store._entities.iterrows():
        if "chebi" not in row["external_ids"]:
            continue
        for chebi_id in row["external_ids"]["chebi"]:
            key = int(chebi_id)
            if key not in chebi2fa:
                chebi2fa[key] = []
            chebi2fa[key].append(str(faid))
    return chebi2fa
