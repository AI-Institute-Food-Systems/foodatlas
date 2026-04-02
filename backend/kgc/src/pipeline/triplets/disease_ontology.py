"""Build disease ontology triplets (is_a) from Phase 1 CTD edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def create_disease_ontology(
    entity_store: EntityStore,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> pd.DataFrame:
    """Generate is_a triplets from Phase 1 CTD disease hierarchy edges."""
    ctd = sources.get("ctd")
    if ctd is None:
        return pd.DataFrame()

    edges = ctd["edges"]
    is_a_edges = edges[edges["edge_type"] == "is_a"]
    disease2fa = _build_disease_to_fa_map(entity_store)

    rows: list[dict[str, str | None]] = []
    for _, edge in is_a_edges.iterrows():
        head = str(edge["head_native_id"])
        tail = str(edge["tail_native_id"])
        if head in disease2fa and tail in disease2fa:
            rows.append(
                {
                    "foodatlas_id": None,
                    "head_id": disease2fa[head],
                    "relationship_id": "r2",
                    "tail_id": disease2fa[tail],
                    "source": "ctd",
                }
            )

    disease_ontology = pd.DataFrame(rows)
    if not disease_ontology.empty:
        disease_ontology["foodatlas_id"] = [
            f"do{i}" for i in range(1, len(disease_ontology) + 1)
        ]

    logger.info("Created %d disease ontology triplets.", len(disease_ontology))
    return disease_ontology


def _build_disease_to_fa_map(entity_store: EntityStore) -> dict[str, str]:
    disease2fa: dict[str, str] = {}
    for faid, row in entity_store._entities.iterrows():
        if "ctd" not in row["external_ids"]:
            continue
        for ctd_id in row["external_ids"]["ctd"]:
            disease2fa[ctd_id] = str(faid)
    return disease2fa
