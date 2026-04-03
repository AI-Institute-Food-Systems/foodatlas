"""Build chemical ontology triplets (is_a) from Phase 1 ChEBI edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def create_chemical_ontology(
    entity_store: EntityStore,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> pd.DataFrame:
    """Generate is_a triplets from Phase 1 ChEBI edges."""
    chebi = sources.get("chebi")
    if chebi is None:
        return pd.DataFrame()

    edges = chebi["edges"]
    is_a_edges = edges[edges["edge_type"] == "is_a"]
    chebi2fa = _build_chebi_to_fa_map(entity_store)

    rows: list[dict[str, str]] = []
    for _, edge in is_a_edges.iterrows():
        head_key = int(edge["head_native_id"])
        tail_key = int(edge["tail_native_id"])
        head_ids = chebi2fa.get(tail_key, [])
        tail_ids = chebi2fa.get(head_key, [])
        if not head_ids or not tail_ids:
            continue
        for head_id in head_ids:
            for tail_id in tail_ids:
                rows.append(
                    {
                        "head_id": head_id,
                        "relationship_id": "r2",
                        "tail_id": tail_id,
                        "source": "chebi",
                    }
                )

    is_a = pd.DataFrame(rows)

    logger.info("Created %d chemical ontology triplets.", len(is_a))
    return is_a


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
