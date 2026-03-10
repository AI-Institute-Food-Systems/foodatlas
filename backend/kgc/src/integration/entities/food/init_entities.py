"""Initialize food entities from FoodOn and FDC."""

import logging
from typing import TYPE_CHECKING

import pandas as pd
from inflection import pluralize, singularize

from ....models.entity import FoodEntity
from ....stores.entity_store import EntityStore
from .loaders import load_fdc, load_foodon, load_lut_food

if TYPE_CHECKING:
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)


def append_foods_from_foodon(
    entity_store: EntityStore,
    settings: "KGCSettings",
) -> None:
    """Create food entities from FoodOn with unique synonym sets."""
    logger.info("Initializing food entities from FoodOn.")

    foodon = load_foodon(settings)
    foodon_food = foodon[foodon["is_food"]].sort_index()
    lut_food = load_lut_food(settings)
    lut_food_df = pd.DataFrame(lut_food.items(), columns=["name", "foodon_id"])
    names_grouped = lut_food_df.groupby("foodon_id")["name"].apply(list)

    entities_new_rows = []
    for foodon_id, _ in foodon_food.iterrows():
        synonyms = names_grouped[foodon_id]
        entity = FoodEntity(
            foodatlas_id=f"e{entity_store._curr_eid}",
            common_name=synonyms[0],
            synonyms=synonyms,
            external_ids={"foodon": [foodon_id]},
            synonyms_display=_remove_plural_display(synonyms),
        )
        entities_new_rows.append(entity.model_dump(by_alias=True))
        entity_store._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index("foodatlas_id")

    entity_store._entities = pd.concat([entity_store._entities, entities_new])

    _rebuild_food_lut(entity_store, entities_new, lut_food_df)
    logger.info("Added %d unique food entities from FoodOn.", len(entities_new))


def _remove_plural_display(synonyms: list[str]) -> dict[str, list[str]]:
    """Heuristic to strip plural forms appended at the end of synonyms."""
    if len(synonyms) == 1:
        return {"foodon": synonyms}

    for i, s in enumerate(synonyms):
        for form_fn in (pluralize, singularize):
            form = form_fn(s)
            try:
                i_end = synonyms[i + 1 :].index(form) + i + 1
                return {"foodon": synonyms[:i_end]}
            except ValueError:
                continue

    return {"foodon": synonyms}


def _rebuild_food_lut(
    entity_store: EntityStore,
    entities_new: pd.DataFrame,
    lut_food_df: pd.DataFrame,
) -> None:
    """Rebuild the food LUT mapping names -> entity IDs."""
    foodon2fa: dict[str, str] = {}
    for entity_id, row in entities_new.iterrows():
        foodon2fa[row["external_ids"]["foodon"][0]] = str(entity_id)

    entity_store._lut_food = {}
    for _, row in lut_food_df.iterrows():
        entity_store._lut_food[row["name"]] = [foodon2fa[row["foodon_id"]]]


def append_foods_from_fdc(
    entity_store: EntityStore,
    settings: "KGCSettings",
) -> None:
    """Add FDC food entities, linking to existing FoodOn entities where possible."""
    fdc = load_fdc(settings).sort_index()

    foodon2fa: dict[str, str] = {}
    for entity_id, row in entity_store._entities.iterrows():
        if "foodon" in row["external_ids"]:
            foodon_id = row["external_ids"]["foodon"][0]
            if foodon_id in foodon2fa:
                msg = "Duplicate FoodOn ID."
                raise ValueError(msg)
            foodon2fa[foodon_id] = str(entity_id)

    n_linked = 0
    entities_not_added = []
    for fdc_id, row in fdc.iterrows():
        foodon_id = row["foodon_url"]
        if foodon_id not in foodon2fa:
            entities_not_added.append(row)
            continue
        fa_id = foodon2fa[foodon_id]
        if "fdc" not in entity_store._entities.at[fa_id, "external_ids"]:
            entity_store._entities.at[fa_id, "external_ids"]["fdc"] = []
        entity_store._entities.at[fa_id, "external_ids"]["fdc"].append(fdc_id)
        n_linked += 1

    logger.info("Linked %d FDC foods to existing entities.", n_linked)

    entities_not_added_df = pd.DataFrame(entities_not_added)
    entities_new_rows = []
    for fdc_id, row in entities_not_added_df.iterrows():
        entity = FoodEntity(
            foodatlas_id=f"e{entity_store._curr_eid}",
            common_name=row["description"],
            synonyms=[row["description"]],
            external_ids={"fdc": [fdc_id]},
            synonyms_display={"fdc": [row["description"]]},
        )
        entities_new_rows.append(entity.model_dump(by_alias=True))
        entity_store._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index("foodatlas_id")
    entity_store._entities = pd.concat([entity_store._entities, entities_new])

    for entity_id, row in entities_new.iterrows():
        entity_store._lut_food[row["common_name"]] = [str(entity_id)]

    logger.info("Added %d new food entities from FDC.", len(entities_new))
