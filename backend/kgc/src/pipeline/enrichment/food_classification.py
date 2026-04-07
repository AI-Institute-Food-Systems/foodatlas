"""Food classification via FoodOn IS_A hierarchy traversal.

Assigns each food entity to one or more categories by checking
whether it is a descendant of curated FoodOn anchor nodes.  Multi-label:
a single food can belong to multiple categories (e.g. a fruit juice is
both "botanical fruit food product" and "plant derived beverage").

Note: FoodOn IS_A direction is head=child, tail=parent — the inverse of
the chemical (ChEBI) convention.  ``_build_parent_child_map`` accounts
for this by mapping tail -> head.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ...models.attributes import FoodAttributes
from .utils import read_attributes, write_attributes

if TYPE_CHECKING:
    import pandas as pd

    from ...stores.entity_store import EntityStore
    from ...stores.triplet_store import TripletStore

logger = logging.getLogger(__name__)

# Each entry maps a human-readable category name (the FoodOn entity's
# ``common_name``) to the ``foodatlas_id`` of its anchor entity in the
# KG.  Every descendant of that anchor (via IS_A edges) inherits the
# category label.
#
# Broad categories (level 1 — children of root "food product by organism"):
#   Every food reachable in the IS_A tree gets at least one of these.
# Specific categories (level 2/3 under plant / animal):
#   Finer-grained labels that coexist with the broad label.
FOOD_CATEGORIES: dict[str, str] = {
    # Broad
    "plant food product": "e19",
    "animal food product": "e2032",
    "fungus food product": "e135",
    "algal food product": "e170",
    # Specific — plant (level 2)
    "plant fruit food product": "e59",
    "plant seed or nut food product": "e10145",
    "spice or herb": "e217",
    # Specific — animal (level 3, children of "vertebrate animal food product")
    "dairy food product": "e231",
    "mammalian meat food product": "e10",
    "fish food product": "e223",
    "avian food product": "e226",
    "egg food product": "e246",
    # Specific — animal (level 2)
    "animal seafood product": "e49",
}

_IS_A_RELATIONSHIP = "r2"


def classify_foods(
    entity_store: EntityStore,
    triplet_store: TripletStore,
) -> None:
    """Assign ``food_groups`` in entity attributes.

    Writes into the ``attributes`` column via :class:`FoodAttributes`.
    An empty list means the food could not be classified.
    """
    entities = entity_store._entities
    food_mask = entities["entity_type"] == "food"
    food_ids = set(entities.index[food_mask])

    parent_to_children = _build_parent_child_map(triplet_store._triplets, food_ids)

    category_members: dict[str, set[str]] = {}
    for cat_name, anchor_eid in FOOD_CATEGORIES.items():
        descendants = _get_all_descendants(anchor_eid, parent_to_children)
        descendants.add(anchor_eid)
        category_members[cat_name] = descendants & food_ids

    classified = 0
    for eid in food_ids:
        matched = sorted(
            cat for cat, members in category_members.items() if eid in members
        )
        attrs = read_attributes(entities, eid, FoodAttributes)
        attrs.food_groups = matched
        write_attributes(entities, eid, attrs)
        if matched:
            classified += 1

    logger.info(
        "Classified %d / %d foods across %d categories.",
        classified,
        len(food_ids),
        len(FOOD_CATEGORIES),
    )


def _build_parent_child_map(
    triplets: pd.DataFrame,
    food_ids: set[str],
) -> dict[str, set[str]]:
    """Build parent -> children mapping from IS_A triplets.

    In the KG, food IS_A triplets are stored as ``head=child,
    tail=parent`` (the inverse of the chemical convention).
    """
    is_a = triplets[triplets["relationship_id"] == _IS_A_RELATIONSHIP]
    is_a_ff = is_a[is_a["head_id"].isin(food_ids) & is_a["tail_id"].isin(food_ids)]

    parent_to_children: dict[str, set[str]] = {}
    for _, row in is_a_ff.iterrows():
        parent_to_children.setdefault(row["tail_id"], set()).add(row["head_id"])
    return parent_to_children


def _get_all_descendants(
    node: str,
    children_map: dict[str, set[str]],
) -> set[str]:
    """Iterative DFS to collect all descendants of *node*."""
    result: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        for child in children_map.get(current, ()):
            if child not in result:
                result.add(child)
                stack.append(child)
    return result
