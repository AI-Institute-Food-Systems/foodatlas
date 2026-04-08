"""Build chemical-disease triplets from Phase 1 CTD edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.relationship import RelationshipType
from ..utils import explode_external_ids

if TYPE_CHECKING:
    from ...knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_EVIDENCE_TO_REL: dict[str, str] = {
    "marker/mechanism": RelationshipType.POSITIVELY_CORRELATES_WITH,
    "therapeutic": RelationshipType.NEGATIVELY_CORRELATES_WITH,
}


def merge_ctd_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create chemical-disease triplets from CTD direct-evidence edges."""
    ctd = sources.get("ctd")
    if ctd is None:
        return
    edges = ctd["edges"]
    chemdis = edges[edges["edge_type"] == "chemical_disease_association"]
    logger.info("Filtering %d CTD chemdis edges for direct evidence...", len(chemdis))

    # raw_attrs may be dicts (in-memory) or JSON strings (from parquet).
    sample = chemdis["raw_attrs"].iloc[0] if len(chemdis) > 0 else None
    if isinstance(sample, str):
        # Fast string filter — match non-empty direct_evidence values.
        has_evidence = chemdis["raw_attrs"].str.contains(
            '"direct_evidence": "[^"]', na=False, regex=True
        )
        direct = chemdis[has_evidence].copy()
        direct["raw_attrs"] = direct["raw_attrs"].apply(json.loads)
    else:
        direct = chemdis[
            chemdis["raw_attrs"].apply(lambda x: bool(x.get("direct_evidence")))
        ].copy()

    if direct.empty:
        logger.info("No direct CTD chemical-disease edges.")
        return
    logger.info("Found %d direct-evidence edges.", len(direct))

    # Map relationship type.
    direct["_rel_id"] = direct["raw_attrs"].apply(
        lambda x: _EVIDENCE_TO_REL.get(x.get("direct_evidence", ""))
    )
    direct = direct[direct["_rel_id"].notna()]

    # Build ID maps as DataFrames for vectorized join.
    mesh2fa = explode_external_ids(kg.entities._entities, "mesh")
    disease2fa = explode_external_ids(kg.entities._entities, "ctd")

    # Join head (chemical via MeSH).
    df = direct.merge(
        mesh2fa, left_on="head_native_id", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_head_id", "candidates": "head_candidates"}
    )

    # Join tail (disease via CTD ID).
    df = df.merge(
        disease2fa, left_on="tail_native_id", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_tail_id", "candidates": "tail_candidates"}
    )

    if df.empty:
        logger.info("No CTD data to merge after resolution.")
        return

    # Explode PubMedIDs so each PMID is its own evidence record.
    df["_pmids"] = df["raw_attrs"].apply(lambda x: x.get("PubMedIDs", []))
    df["_de"] = df["raw_attrs"].apply(lambda x: x.get("direct_evidence", ""))
    exploded = df.explode("_pmids", ignore_index=True)
    exploded = exploded[exploded["_pmids"].notna() & (exploded["_pmids"] != "")]

    if exploded.empty:
        logger.info("No CTD edges with PubMed IDs after explode.")
        return

    exploded["source_type"] = "ctd"
    exploded["reference"] = exploded.apply(
        lambda r: json.dumps(
            {"ctd_direct_evidence": r["_de"], "pmid": str(r["_pmids"])}
        ),
        axis=1,
    )
    exploded["source"] = "ctd"
    exploded["head_name_raw"] = exploded["head_native_id"].astype(str)
    exploded["tail_name_raw"] = exploded["tail_native_id"].astype(str)

    ev_result = kg.evidence.create(exploded[["source_type", "reference"]])
    exploded["evidence_id"] = ev_result.index
    attestations = kg.attestations.create(exploded)

    triplet_input = exploded[["_head_id", "_tail_id", "_rel_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id", "relationship_id"])
    triplet_input.index = attestations.index
    triplets = kg.triplets.create(triplet_input)

    logger.info(
        "Merged %d CTD attestations, %d triplets.", len(attestations), len(triplets)
    )
