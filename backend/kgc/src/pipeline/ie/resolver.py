"""Resolve IE metadata food/chemical names against entity LUTs."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd

from ...models.relationship import RelationshipType

if TYPE_CHECKING:
    from ...stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


@dataclass
class IEResolutionResult:
    """Outcome of resolving IE metadata against entity LUTs."""

    resolved: pd.DataFrame
    unresolved_food: set[str] = field(default_factory=set)
    unresolved_chemical: set[str] = field(default_factory=set)
    stats: dict[str, Any] = field(default_factory=dict)


def resolve_ie_metadata(
    metadata: pd.DataFrame,
    entity_store: EntityStore,
) -> IEResolutionResult:
    """Resolve food/chemical names via LUT lookup only.

    Args:
        metadata: DataFrame with ``_food_name`` and ``_chemical_name`` columns.
        entity_store: Loaded entity store with populated LUTs.

    Returns:
        :class:`IEResolutionResult` with resolved rows, unresolved names, and stats.
    """
    if metadata.empty:
        return IEResolutionResult(
            resolved=pd.DataFrame(),
            stats=_empty_stats(),
        )

    unique_foods = set(metadata["_food_name"].unique())
    unique_chems = set(metadata["_chemical_name"].unique())

    # Build name → entity_ids lookup caches.
    food_map = {n: entity_store.get_entity_ids("food", n) for n in unique_foods}
    chem_map = {n: entity_store.get_entity_ids("chemical", n) for n in unique_chems}

    unresolved_food = {n for n, ids in food_map.items() if not ids}
    unresolved_chem = {n for n, ids in chem_map.items() if not ids}

    ambiguous_foods = {n: ids for n, ids in food_map.items() if len(ids) > 1}
    ambiguous_chems = {n: ids for n, ids in chem_map.items() if len(ids) > 1}

    # Map entity IDs onto metadata rows.
    df = metadata.copy()
    df["head_id"] = df["_food_name"].map(food_map)
    df["tail_id"] = df["_chemical_name"].map(chem_map)
    df["head_candidates"] = df["head_id"]  # full candidate lists before explode
    df["tail_candidates"] = df["tail_id"]

    # Keep only rows where both resolved.
    resolved = df[df["head_id"].apply(bool) & df["tail_id"].apply(bool)].copy()
    pre_explode = len(resolved)
    resolved = resolved.explode("head_id").explode("tail_id")
    resolved["relationship_id"] = RelationshipType.CONTAINS

    stats = {
        "total_ie_rows": len(metadata),
        "resolved_rows": len(resolved),
        "dropped_rows": len(metadata)
        - len(df[df["head_id"].apply(bool) & df["tail_id"].apply(bool)]),
        "unique_food_names": len(unique_foods),
        "unique_chemical_names": len(unique_chems),
        "resolved_food_names": len(unique_foods) - len(unresolved_food),
        "resolved_chemical_names": len(unique_chems) - len(unresolved_chem),
        "unresolved_food_names": len(unresolved_food),
        "unresolved_chemical_names": len(unresolved_chem),
        "ambiguous_food_names": len(ambiguous_foods),
        "ambiguous_chemical_names": len(ambiguous_chems),
        "rows_from_ambiguity": len(resolved) - pre_explode,
    }

    logger.info(
        "IE resolution: %d/%d rows resolved, %d/%d food names, %d/%d chemical names.",
        stats["resolved_rows"],
        stats["total_ie_rows"],
        stats["resolved_food_names"],
        stats["unique_food_names"],
        stats["resolved_chemical_names"],
        stats["unique_chemical_names"],
    )

    return IEResolutionResult(
        resolved=resolved,
        unresolved_food=unresolved_food,
        unresolved_chemical=unresolved_chem,
        stats=stats,
    )


def _empty_stats() -> dict[str, Any]:
    return {
        "total_ie_rows": 0,
        "resolved_rows": 0,
        "dropped_rows": 0,
        "unique_food_names": 0,
        "unique_chemical_names": 0,
        "resolved_food_names": 0,
        "resolved_chemical_names": 0,
        "unresolved_food_names": 0,
        "unresolved_chemical_names": 0,
        "ambiguous_food_names": 0,
        "ambiguous_chemical_names": 0,
        "rows_from_ambiguity": 0,
    }
