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

    rows: list[dict[str, str]] = []
    for _, edge in is_a_edges.iterrows():
        head_ids = disease2fa.get(str(edge["head_native_id"]), [])
        tail_ids = disease2fa.get(str(edge["tail_native_id"]), [])
        if not head_ids or not tail_ids:
            continue
        for head_id in head_ids:
            for tail_id in tail_ids:
                rows.append(
                    {
                        "head_id": head_id,
                        "relationship_id": "r2",
                        "tail_id": tail_id,
                        "source": "ctd",
                    }
                )

    disease_ontology = pd.DataFrame(rows)

    logger.info("Created %d disease ontology triplets.", len(disease_ontology))
    return disease_ontology


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
