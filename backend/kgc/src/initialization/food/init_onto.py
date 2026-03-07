"""Initialize food ontology triplets from FoodOn hierarchy."""

import json
import logging
from pathlib import Path

import pandas as pd

from ...models.settings import KGCSettings
from ...stores.entity_store import EntityStore
from ...stores.schema import FILE_FOOD_ONTOLOGY
from .loaders import load_foodon

logger = logging.getLogger(__name__)


def create_food_ontology(
    entity_store: EntityStore,
    settings: KGCSettings,
) -> pd.DataFrame:
    """Traverse FoodOn hierarchy to generate is_a triplets.

    Returns the ontology DataFrame and saves it to the KG directory.
    """
    foodon = load_foodon(settings)
    foodon_food = foodon[foodon["is_food"]]

    foodon2fa = _build_foodon_to_fa_map(entity_store)
    ontology_rows = _traverse_hierarchy(foodon_food, foodon2fa)

    food_ontology = pd.DataFrame(ontology_rows)
    food_ontology["foodatlas_id"] = [f"fo{i}" for i in range(1, len(food_ontology) + 1)]

    kg_dir = Path(settings.kg_dir)
    records = food_ontology.to_dict(orient="records")
    with (kg_dir / FILE_FOOD_ONTOLOGY).open("w") as f:
        json.dump(records, f, ensure_ascii=False)
    logger.info("Created %d food ontology triplets.", len(food_ontology))

    return food_ontology


def _build_foodon_to_fa_map(entity_store: EntityStore) -> dict[str, str]:
    """Map FoodOn IDs to FoodAtlas entity IDs."""
    foodon2fa: dict[str, str] = {}
    for faid, row in entity_store._entities.iterrows():
        if "foodon" not in row["external_ids"]:
            continue
        foodon2fa[row["external_ids"]["foodon"][0]] = str(faid)
    return foodon2fa


def _traverse_hierarchy(
    foodon_food: pd.DataFrame,
    foodon2fa: dict[str, str],
) -> list[dict[str, str | None]]:
    """BFS traversal of FoodOn hierarchy to collect is_a relationships."""
    ontology_rows: list[dict[str, str | None]] = []
    visited: set[str] = set()

    for foodon_id in foodon_food.index:
        queue = [foodon_id]
        while queue:
            current = queue.pop()
            if current in visited:
                continue
            visited.add(current)

            for parent in foodon_food.loc[current, "parents"]:
                if parent in foodon_food.index:
                    queue.append(parent)
                    ontology_rows.append(
                        {
                            "foodatlas_id": None,
                            "head_id": foodon2fa[current],
                            "relationship_id": "r2",
                            "tail_id": foodon2fa[parent],
                            "source": "foodon",
                        }
                    )

    return ontology_rows
