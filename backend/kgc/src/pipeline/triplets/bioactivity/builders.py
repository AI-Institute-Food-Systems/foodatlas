"""Build bioactivity triplets: food/chemical → bioactivity, and the concept
hierarchy.

Association edges (``exhibits`` r5, ``measured`` r6) carry their per-measurement
``bm…`` ids as ``attestation_ids`` — the measurement detail itself lives in
``attestations_bioactivity.parquet`` (the bioactivity attestation store; see
:mod:`.measurements`), not in the generic ``attestations.parquet``. The
hierarchy (``is_a`` r2) mirrors the other ontology builders, creating provenance
attestations for its handful of edges.
"""

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

_SOURCE = "bioactivity"
_TAIL_KEY = "bioactivity_concept"  # external_ids key on bioactivity entities


def merge_food_bioactivity(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create food --exhibits--> bioactivity (r5) triplets."""
    _merge_association(kg, sources, "exhibits", RelationshipType.EXHIBITS, "food")


def merge_chemical_bioactivity(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create chemical --measured--> bioactivity (r6) triplets."""
    _merge_association(kg, sources, "measured", RelationshipType.MEASURED, "chemical")


def _merge_association(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
    edge_type: str,
    rel_id: str,
    head_kind: str,
) -> None:
    bioactivity = sources.get("bioactivity")
    if bioactivity is None:
        return
    edges = bioactivity["edges"]
    edges = edges[edges["edge_type"] == edge_type]
    if edges.empty:
        return

    df = _resolve_tail(kg, edges)
    df = _resolve_head(kg, df, head_kind)
    if df.empty:
        logger.info("No %s edges resolved.", edge_type)
        return

    metadata = _explode_measurements(df, rel_id)
    kg.triplets.create(metadata)
    logger.info(
        "Bioactivity %s: %d triplets, %d measurement links.",
        edge_type,
        df.groupby(["head_id", "tail_id"]).ngroups,
        len(metadata),
    )


def _resolve_tail(kg: KnowledgeGraph, edges: pd.DataFrame) -> pd.DataFrame:
    """Resolve the bioactivity-concept tail via external_ids; drops UNCLASSIFIED."""
    lookup = explode_external_ids(kg.entities._entities, _TAIL_KEY)
    df = edges.merge(
        lookup, left_on="tail_native_id", right_on="native_id", how="inner"
    )
    return df.rename(columns={"foodatlas_id": "tail_id"}).drop(
        columns=["native_id", "candidates"]
    )


def _resolve_head(kg: KnowledgeGraph, df: pd.DataFrame, head_kind: str) -> pd.DataFrame:
    """Resolve the head: food by raw foodatlas_id (with existence guard),
    chemical by PubChem CID via external_ids."""
    if df.empty:
        return df
    if head_kind == "food":
        entities = kg.entities._entities
        valid = set(entities.index[entities["entity_type"] == "food"])
        df = df.assign(head_id=df["head_native_id"])
        resolved = df[df["head_id"].isin(valid)]
        orphans = len(df) - len(resolved)
        if orphans:
            logger.warning(
                "Dropped %d food edges whose head is not a known food entity.",
                orphans,
            )
        return resolved

    lookup = explode_external_ids(kg.entities._entities, "pubchem_compound")
    df = df.merge(lookup, left_on="head_native_id", right_on="native_id", how="inner")
    return df.rename(columns={"foodatlas_id": "head_id"}).drop(
        columns=["native_id", "candidates"]
    )


def _explode_measurements(df: pd.DataFrame, rel_id: str) -> pd.DataFrame:
    """One row per (triplet, bm id), indexed by bm id so create() appends them
    as the triplet's attestation_ids."""
    df = df.assign(
        _bm=df["raw_attrs"].apply(
            lambda a: (
                a.get("bioactivity_metadata_ids", []) if isinstance(a, dict) else []
            )
        )
    )
    exploded = df.explode("_bm").dropna(subset=["_bm"])
    metadata = pd.DataFrame(
        {
            "head_id": exploded["head_id"].to_numpy(),
            "relationship_id": str(rel_id),
            "tail_id": exploded["tail_id"].to_numpy(),
            "source": _SOURCE,
        },
        index=exploded["_bm"].to_numpy(),
    )
    return metadata


def merge_bioactivity_ontology(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create bioactivity is_a bioactivity (r2) triplets from the hierarchy."""
    bioactivity = sources.get("bioactivity")
    if bioactivity is None:
        return
    is_a = bioactivity["edges"]
    is_a = is_a[is_a["edge_type"] == "is_a"]
    if is_a.empty:
        return

    lookup = explode_external_ids(kg.entities._entities, _TAIL_KEY)
    if lookup.empty:
        return

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
        logger.info("No bioactivity is_a edges to merge.")
        return

    df["source_type"] = _SOURCE
    df["reference"] = json.dumps({"source": _SOURCE, "edge_type": "is_a"})
    df["source"] = _SOURCE
    df["head_name_raw"] = df["head_native_id"].astype(str)
    df["tail_name_raw"] = df["tail_native_id"].astype(str)

    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    attestations = kg.attestations.create(df)

    triplet_input = df[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id"])
    triplet_input.index = attestations.index
    triplet_input["relationship_id"] = str(RelationshipType.IS_A)
    triplets = kg.triplets.create(triplet_input)

    logger.info("Created %d bioactivity is_a triplets.", len(triplets))
