"""Chemical classification via ChEBI IS_A hierarchy traversal.

Assigns each chemical entity to zero or more categories by checking
whether it is a descendant of curated ChEBI anchor nodes.  Multi-label:
a single chemical can belong to multiple categories (e.g. a tannin is
also a polyphenol).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ...models.attributes import ChemicalAttributes
from .utils import read_attributes, write_attributes

if TYPE_CHECKING:
    import pandas as pd

    from ...stores.entity_store import EntityStore
    from ...stores.triplet_store import TripletStore

logger = logging.getLogger(__name__)

# Each entry maps a human-readable category name to the ``foodatlas_id``
# of its ChEBI anchor entity in the KG.  Every descendant of that anchor
# (via IS_A edges) inherits the category label.
CHEMICAL_CATEGORIES: dict[str, str] = {
    "flavonoid": "e65128",
    "stilbenoid": "e13442",
    "lignan": "e62440",
    "tannin": "e13486",
    "polyphenol": "e12163",
    "terpenoid": "e11453",
    "alkaloid": "e12451",
    "glucosinolate": "e10934",
    "carbohydrate": "e60839",
    "amino acid": "e64236",
    "fatty acid": "e64324",
    "nucleotide": "e64474",
}

_IS_A_RELATIONSHIP = "r2"


def classify_chemicals(
    entity_store: EntityStore,
    triplet_store: TripletStore,
) -> None:
    """Assign ``chemical_groups`` in entity attributes.

    Writes into the ``attributes`` column via :class:`ChemicalAttributes`.
    An empty list means the chemical could not be classified.
    """
    entities = entity_store._entities
    chem_mask = entities["entity_type"] == "chemical"
    chem_ids = set(entities.index[chem_mask])

    parent_to_children = _build_parent_child_map(triplet_store._triplets, chem_ids)

    # Pre-compute descendant sets for each category anchor.
    category_members: dict[str, set[str]] = {}
    for cat_name, anchor_eid in CHEMICAL_CATEGORIES.items():
        descendants = _get_all_descendants(anchor_eid, parent_to_children)
        descendants.add(anchor_eid)
        category_members[cat_name] = descendants & chem_ids

    # Assign labels via attributes.
    classified = 0
    for eid in chem_ids:
        matched = sorted(
            cat for cat, members in category_members.items() if eid in members
        )
        attrs = read_attributes(entities, eid, ChemicalAttributes)
        attrs.chemical_groups = matched
        write_attributes(entities, eid, attrs)
        if matched:
            classified += 1

    logger.info(
        "Classified %d / %d chemicals across %d categories.",
        classified,
        len(chem_ids),
        len(CHEMICAL_CATEGORIES),
    )


def _build_parent_child_map(
    triplets: pd.DataFrame,
    chem_ids: set[str],
) -> dict[str, set[str]]:
    """Build parent -> children mapping from IS_A triplets.

    IS_A triplets use natural direction: ``head=child, tail=parent``.
    """
    is_a = triplets[triplets["relationship_id"] == _IS_A_RELATIONSHIP]
    is_a_cc = is_a[is_a["head_id"].isin(chem_ids) & is_a["tail_id"].isin(chem_ids)]

    parent_to_children: dict[str, set[str]] = {}
    for _, row in is_a_cc.iterrows():
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
