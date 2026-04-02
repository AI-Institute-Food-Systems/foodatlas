"""Food entity grouping via FoodOn ontology hierarchy."""

import logging
from pathlib import Path

import pandas as pd

from ....models.settings import KGCSettings
from ....stores.entity_store import EntityStore
from ....utils.json_io import read_json

logger = logging.getLogger(__name__)

_FOOD_GROUP_NAMES: dict[str, str] = {
    "dairy food product": "dairy",
    "plant fruit food product": "fruit",
    "plant seed or nut food product": "plant seed or nut",
    "legume food product": "legume",
    "vegetable food product": "vegetable",
    "plant food product": "other plant",
    "mammalian meat food product": "mammalian meat",
    "avian food product": "avian",
    "animal seafood product": "seafood",
    "fish food product": "seafood",
    "animal food product": "other animal",
}


def generate_food_groups_foodon(
    entity_store: EntityStore,
    settings: KGCSettings,
    level: int = 1,
) -> pd.Series:
    """Assign FoodOn-based food groups to food entities.

    Args:
        entity_store: Store with entities and food LUT.
        settings: Settings providing ``kg_dir``.
        level: Hierarchy depth for group resolution (1 or 2).

    Returns:
        Series indexed by entity ID with list[str] group labels.
    """
    foods = entity_store._entities.query("entity_type == 'food'").copy()
    kg_dir = Path(settings.kg_dir)

    foodonto = _load_food_ontology(kg_dir)
    ht_is_a = _build_is_a_map(foodonto)
    ht_has_child = _invert_is_a(ht_is_a)

    group_eids = _resolve_group_eids(entity_store, _FOOD_GROUP_NAMES)
    ht = _build_group_mapping(group_eids, _FOOD_GROUP_NAMES, ht_has_child, foods, level)

    _traverse_hierarchy(foods, ht, ht_is_a)

    foods["foodon"] = foods.index.map(lambda x: clean_groups(ht.get(x, [])))
    return foods["foodon"]


def _load_food_ontology(kg_dir: Path) -> pd.DataFrame:
    df = pd.DataFrame(read_json(kg_dir / "food_ontology.json"))
    return df[df["source"] == "foodon"]


def _build_is_a_map(ontology: pd.DataFrame) -> dict[str, list[str]]:
    ht: dict[str, list[str]] = {}
    for _, row in ontology.iterrows():
        head = row["head_id"]
        if head not in ht:
            ht[head] = []
        ht[head].append(row["tail_id"])
    return ht


def _invert_is_a(ht_is_a: dict[str, list[str]]) -> dict[str, set[str]]:
    ht_has_child: dict[str, set[str]] = {}
    for child, parents in ht_is_a.items():
        for parent in parents:
            if parent not in ht_has_child:
                ht_has_child[parent] = set()
            ht_has_child[parent].add(child)
    return ht_has_child


def _resolve_group_eids(
    entity_store: EntityStore,
    group_names: dict[str, str],
) -> list[str]:
    """Look up entity IDs for each food group name via the food LUT."""
    eids = []
    for name in group_names:
        ids = entity_store.get_entity_ids("food", name)
        if ids:
            eids.append(ids[0])
        else:
            logger.warning("Food group '%s' not found in LUT.", name)
    return eids


def _build_group_mapping(
    eids: list[str],
    group_names: dict[str, str],
    ht_has_child: dict[str, set[str]],
    foods: pd.DataFrame,
    level: int,
) -> dict[str, list[str]]:
    """Build eid → group label mapping at the given level."""
    ht: dict[str, list[str]] = {}
    labels = list(group_names.values())

    if level == 1:
        for eid, label in zip(eids, labels, strict=False):
            ht[eid] = [label]
    elif level == 2:
        for eid in eids:
            children = list(ht_has_child.get(eid, set()))
            for child in children:
                if child not in ht:
                    ht[child] = []
                if child in foods.index:
                    ht[child].append(foods.loc[child, "common_name"])
        ht = {k: sorted(set(v)) for k, v in ht.items()}
    else:
        msg = f"Invalid level: {level}"
        raise ValueError(msg)

    return ht


def _traverse_hierarchy(
    foods: pd.DataFrame,
    ht: dict[str, list[str]],
    ht_is_a: dict[str, list[str]],
) -> None:
    """DFS to propagate group labels up through the hierarchy."""

    def dfs(eid: str) -> list[str]:
        if eid in ht:
            return ht[eid]
        res: list[str] = []
        for parent in ht_is_a.get(eid, []):
            res += dfs(parent)
        ht[eid] = sorted(set(res))
        return ht[eid]

    for eid in foods.index:
        dfs(eid)


def clean_groups(groups: list[str]) -> list[str]:
    """Resolve ambiguous multi-group assignments to a single group.

    Applies a priority-based heuristic: more specific groups take
    precedence over broader parent groups.
    """
    if not groups:
        return ["unclassified"]

    groups = list(groups)

    specific_plant = {"fruit", "legume", "plant seed or nut", "vegetable"}
    if "other plant" in groups and specific_plant.intersection(groups):
        groups.remove("other plant")

    specific_animal = {"seafood", "avian", "mammalian meat", "dairy"}
    if "other animal" in groups and specific_animal.intersection(groups):
        groups.remove("other animal")

    more_specific = {"fruit", "legume", "plant seed or nut"}
    if "vegetable" in groups and more_specific.intersection(groups):
        groups.remove("vegetable")

    if "legume" in groups and "plant seed or nut" in groups:
        groups.remove("plant seed or nut")

    if "mammalian meat" in groups and {"avian", "seafood"}.intersection(groups):
        groups.remove("mammalian meat")

    if len(groups) != 1:
        return ["unclassified"]

    return [groups[0]]
