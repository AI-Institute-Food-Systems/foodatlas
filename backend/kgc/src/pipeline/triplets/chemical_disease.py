"""Build chemical-disease triplets from Phase 1 CTD edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
from tqdm import tqdm

from ...models.relationship import RelationshipType

if TYPE_CHECKING:
    from ..knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_EVIDENCE_TO_REL: dict[str, str] = {
    "marker/mechanism": RelationshipType.POSITIVELY_CORRELATES_WITH,
    "therapeutic": RelationshipType.NEGATIVELY_CORRELATES_WITH,
}


def merge_ctd_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create chemical-disease triplets from CTD direct-evidence edges.

    Creates evidence, extraction (with ambiguity candidates), and triplet
    records.  Resolves CTD chemical IDs (MeSH) via ``external_ids["mesh"]``
    and disease IDs via ``external_ids["ctd"]``.
    """
    ctd = sources.get("ctd")
    if ctd is None:
        return
    edges = ctd["edges"]
    chemdis = edges[edges["edge_type"] == "chemical_disease_association"]
    direct = chemdis[
        chemdis["raw_attrs"].apply(lambda x: bool(x.get("direct_evidence")))
    ]
    if direct.empty:
        logger.info("No direct CTD chemical-disease edges.")
        return

    mesh2fa = _build_mesh_to_fa(kg.entities._entities)
    disease2fa = _build_disease_to_fa(kg.entities._entities)

    rows: list[dict] = []
    for _, edge in tqdm(
        direct.iterrows(), total=len(direct), desc="ctd chemdis", leave=False
    ):
        chem_ids = mesh2fa.get(edge["head_native_id"], [])
        disease_ids = disease2fa.get(edge["tail_native_id"], [])
        if not chem_ids or not disease_ids:
            continue

        evidence = edge["raw_attrs"]["direct_evidence"]
        rel_id = _EVIDENCE_TO_REL.get(evidence)
        if rel_id is None:
            continue

        pubmed_ids = edge["raw_attrs"].get("PubMedIDs", [])
        ref = json.dumps({"ctd_direct_evidence": evidence, "pubmed": pubmed_ids})

        for chem_id in chem_ids:
            for disease_id in disease_ids:
                rows.append(
                    {
                        "source_type": "ctd",
                        "reference": ref,
                        "extractor": "ctd",
                        "head_name_raw": str(edge["head_native_id"]),
                        "tail_name_raw": str(edge["tail_native_id"]),
                        "head_candidates": chem_ids,
                        "tail_candidates": disease_ids,
                        "_head_id": chem_id,
                        "_tail_id": disease_id,
                        "_rel_id": rel_id,
                    }
                )

    if not rows:
        logger.info("No CTD data to merge.")
        return

    df = pd.DataFrame(rows)

    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    extractions = kg.extractions.create(df)

    triplet_input = df[["_head_id", "_tail_id", "_rel_id"]].copy()
    triplet_input.columns = ["head_id", "tail_id", "relationship_id"]
    triplet_input.index = extractions.index
    triplets = kg.triplets.create(triplet_input)

    logger.info(
        "Merged %d CTD extractions, %d triplets.", len(extractions), len(triplets)
    )


def _build_mesh_to_fa(entities: pd.DataFrame) -> dict[str, list[str]]:
    """Map MeSH ID → entity IDs for chemicals (may be 1:N)."""
    result: dict[str, list[str]] = {}
    for eid, row in entities.iterrows():
        for mesh_id in row["external_ids"].get("mesh", []):
            if mesh_id not in result:
                result[mesh_id] = []
            result[mesh_id].append(str(eid))
    return result


def _build_disease_to_fa(entities: pd.DataFrame) -> dict[str, list[str]]:
    """Map CTD disease ID → entity IDs."""
    result: dict[str, list[str]] = {}
    for eid, row in entities.iterrows():
        for ctd_id in row["external_ids"].get("ctd", []):
            if ctd_id not in result:
                result[ctd_id] = []
            result[ctd_id].append(str(eid))
    return result
