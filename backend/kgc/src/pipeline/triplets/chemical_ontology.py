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

    rows: list[dict[str, str | None]] = []
    for _, edge in is_a_edges.iterrows():
        head_id = int(edge["head_native_id"])
        tail_id = int(edge["tail_native_id"])
        if head_id in chebi2fa and tail_id in chebi2fa:
            rows.append(
                {
                    "foodatlas_id": None,
                    "head_id": chebi2fa[tail_id],
                    "relationship_id": "r2",
                    "tail_id": chebi2fa[head_id],
                    "source": "chebi",
                }
            )

    is_a = pd.DataFrame(rows)
    if not is_a.empty:
        is_a["foodatlas_id"] = [f"co{i}" for i in range(1, len(is_a) + 1)]

    logger.info("Created %d chemical ontology triplets.", len(is_a))
    return is_a


def _build_chebi_to_fa_map(entity_store: EntityStore) -> dict[int, str]:
    chebi2fa: dict[int, str] = {}
    for faid, row in entity_store._entities.iterrows():
        if "chebi" not in row["external_ids"]:
            continue
        chebi2fa[int(row["external_ids"]["chebi"][0])] = str(faid)
    return chebi2fa
