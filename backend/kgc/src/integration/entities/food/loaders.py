"""Loaders for FoodOn and FDC food data."""

from pathlib import Path

import pandas as pd
from inflection import pluralize, singularize

from ....models.settings import KGCSettings


def load_foodon(settings: KGCSettings) -> pd.DataFrame:
    """Load the cleaned FoodOn ontology from preprocessed parquet."""
    dp_dir = Path(settings.data_cleaning_dir)
    foodon: pd.DataFrame = pd.read_parquet(dp_dir / "foodon_cleaned.parquet")
    if foodon.index.name != "foodon_id":
        foodon = foodon.set_index("foodon_id")
    return foodon


def load_lut_food(
    settings: KGCSettings,
    *,
    resolve_organisms: bool = True,
    resolve_singular_plural_forms: bool = True,
) -> dict[str, str]:
    """Build a synonym -> FoodOn ID lookup table from the FoodOn ontology."""
    foodon = load_foodon(settings)
    lut_food: dict[str, str] = {}

    syn_levels = [
        "label",
        "label (alternative)",
        "synonym (exact)",
        "synonym",
        "synonym (narrow)",
        "synonym (broad)",
    ]

    for level in syn_levels:
        for foodon_id, row in foodon.iterrows():
            if not row["is_food"]:
                continue
            for syn in row["synonyms"][level]:
                key = syn.lower()
                if key not in lut_food:
                    lut_food[key] = str(foodon_id)

    if resolve_organisms:
        _resolve_organisms(foodon, lut_food, syn_levels)

    if resolve_singular_plural_forms:
        _resolve_singular_plural(lut_food)

    return lut_food


def _resolve_organisms(
    foodon: pd.DataFrame,
    lut_food: dict[str, str],
    syn_levels: list[str],
) -> None:
    """Add organism synonyms that map to exactly one food entity."""
    for level in syn_levels:
        for _, row in foodon.iterrows():
            if row["is_food"]:
                continue
            candidates = list(set(list(row["derives"]) + list(row["has_part"])))
            if len(candidates) != 1:
                continue
            for syn in row["synonyms"][level]:
                key = syn.lower()
                if key not in lut_food:
                    lut_food[key] = candidates[0]


def _resolve_singular_plural(lut_food: dict[str, str]) -> None:
    """Add singular/plural forms of existing LUT entries."""
    additions: dict[str, str] = {}
    for name, foodon_id in lut_food.items():
        if not name[-1].isalpha():
            continue
        for form in (singularize(name), pluralize(name)):
            if form not in lut_food and form not in additions:
                additions[form] = foodon_id
    lut_food.update(additions)


def load_fdc(settings: KGCSettings) -> pd.DataFrame:
    """Load and process FDC foundation food entities with FoodOn links."""
    data_dir = Path(settings.data_dir)
    fdc_dir = data_dir / "FDC" / "FoodData_Central_foundation_food_csv_2024-04-18"

    fdc_ids_ff = pd.read_csv(fdc_dir / "foundation_food.csv")["fdc_id"]
    foods: pd.DataFrame = pd.read_csv(
        fdc_dir / "food.csv", usecols=["fdc_id", "description"]
    ).set_index("fdc_id")
    foods = foods[foods.index.isin(fdc_ids_ff)]
    foods["description"] = foods["description"].str.strip().str.lower()

    food_attr = pd.read_csv(fdc_dir / "food_attribute.csv")
    food_attr = food_attr[food_attr["fdc_id"].isin(fdc_ids_ff)].set_index("fdc_id")

    foods["foodon_url"] = foods.apply(
        lambda row: _extract_foodon_url(row, food_attr), axis=1
    )
    return foods[["description", "foodon_url"]]


def _extract_foodon_url(row: pd.Series, food_attr: pd.DataFrame) -> str:
    """Extract the FoodOn URL for a single FDC food entry."""
    attr_name = "FoodOn Ontology ID for FDC Item"
    fdc_id = row.name

    if fdc_id not in food_attr.index:
        if fdc_id == 2512381:
            return "http://purl.obolibrary.org/obo/FOODON_03000273"
        msg = f"FDC item without FoodOn ID exists: {fdc_id}"
        raise ValueError(msg)

    attr = food_attr.loc[fdc_id]
    if isinstance(attr, pd.Series):
        attr = pd.DataFrame(attr).T
    attr = attr.set_index("name")
    urls = attr.query(f"name == '{attr_name}'")["value"].unique().tolist()

    if len(urls) == 0:
        msg = "FDC item without FoodOn ID exists."
        raise ValueError(msg)

    if len(urls) > 1:
        return _resolve_multiple_foodon_urls(fdc_id, urls)

    return str(urls[0])


def _resolve_multiple_foodon_urls(fdc_id: int, urls: list[str]) -> str:
    """Manual fixes for FDC items with multiple FoodOn URLs."""
    fixes: dict[int, str] = {
        323121: "http://purl.obolibrary.org/obo/FOODON_03310577",
        330137: "http://purl.obolibrary.org/obo/FOODON_00004409",
    }
    if fdc_id in fixes:
        return fixes[fdc_id]
    msg = f"Unaddressed multiple FoodOn URLs: {urls}"
    raise ValueError(msg)
