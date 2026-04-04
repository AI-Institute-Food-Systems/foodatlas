"""Build chemical-disease triplets from Phase 1 CTD edges."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

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
    mesh2fa = _explode_external_ids(kg.entities._entities, "mesh")
    disease2fa = _explode_external_ids(kg.entities._entities, "ctd")

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

    # Build evidence references.
    df["source_type"] = "ctd"
    df["reference"] = df["raw_attrs"].apply(
        lambda x: json.dumps(
            {
                "ctd_direct_evidence": x.get("direct_evidence", ""),
                "pubmed": x.get("PubMedIDs", []),
            }
        )
    )
    df["extractor"] = "ctd"
    df["head_name_raw"] = df["head_native_id"].astype(str)
    df["tail_name_raw"] = df["tail_native_id"].astype(str)

    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    extractions = kg.extractions.create(df)

    triplet_input = df[["_head_id", "_tail_id", "_rel_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id", "relationship_id"])
    triplet_input.index = extractions.index
    triplets = kg.triplets.create(triplet_input)

    logger.info(
        "Merged %d CTD extractions, %d triplets.", len(extractions), len(triplets)
    )


def _explode_external_ids(entities: pd.DataFrame, key: str) -> pd.DataFrame:
    """Build a DataFrame mapping native IDs to entity IDs with candidate lists.

    Returns columns: ``native_id``, ``foodatlas_id``, ``candidates``.
    ``candidates`` is the full list of entity IDs for that native ID.
    """
    rows: list[tuple[str, str]] = []
    for eid, row in entities.iterrows():
        for native_id in row["external_ids"].get(key, []):
            rows.append((str(native_id), str(eid)))
    if not rows:
        return pd.DataFrame(columns=["native_id", "foodatlas_id", "candidates"])

    lookup = pd.DataFrame(rows, columns=["native_id", "foodatlas_id"])
    # Add candidate lists (all entity IDs per native_id).
    candidates = lookup.groupby("native_id")["foodatlas_id"].apply(list)
    candidates.name = "candidates"
    return lookup.merge(candidates, left_on="native_id", right_index=True)
