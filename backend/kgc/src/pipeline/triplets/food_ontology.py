"""Build food ontology triplets (is_a) from Phase 1 FoodOn edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def create_food_ontology(
    entity_store: EntityStore,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> pd.DataFrame:
    """Generate is_a triplets from Phase 1 FoodOn edges."""
    foodon = sources.get("foodon")
    if foodon is None:
        return pd.DataFrame()

    edges = foodon["edges"]
    is_a_edges = edges[edges["edge_type"] == "is_a"]
    foodon2fa = _build_foodon_to_fa_map(entity_store)

    rows: list[dict[str, str | None]] = []
    for _, edge in is_a_edges.iterrows():
        head = edge["head_native_id"]
        tail = edge["tail_native_id"]
        if head in foodon2fa and tail in foodon2fa:
            rows.append(
                {
                    "foodatlas_id": None,
                    "head_id": foodon2fa[head],
                    "relationship_id": "r2",
                    "tail_id": foodon2fa[tail],
                    "source": "foodon",
                }
            )

    food_ontology = pd.DataFrame(rows)
    if not food_ontology.empty:
        food_ontology["foodatlas_id"] = [
            f"fo{i}" for i in range(1, len(food_ontology) + 1)
        ]

    logger.info("Created %d food ontology triplets.", len(food_ontology))
    return food_ontology


def _build_foodon_to_fa_map(entity_store: EntityStore) -> dict[str, str]:
    foodon2fa: dict[str, str] = {}
    for faid, row in entity_store._entities.iterrows():
        if "foodon" not in row["external_ids"]:
            continue
        foodon2fa[row["external_ids"]["foodon"][0]] = str(faid)
    return foodon2fa
