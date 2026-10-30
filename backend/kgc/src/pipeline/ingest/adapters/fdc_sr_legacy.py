"""SR Legacy slice of USDA FoodData Central.

SR Legacy is the frozen (April 2019) successor of the USDA Standard Reference
database: 7,793 foods, each a specific cut, fat level or preparation. Unlike
Foundation Foods they are ingested as their own food nodes and are never
cross-referenced to FoodOn, so Pass 3 creates one food entity per entry. USDA's
own FoodOn and NCBI mapping is kept on the node as informational attributes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from .fdc_source import SOURCE_ID

if TYPE_CHECKING:
    from pathlib import Path

    from ..protocol import ProgressCallback

logger = logging.getLogger(__name__)

DATA_TYPE = "sr_legacy_food"
_NUTRIENT_CHUNK_ROWS = 1_000_000

# food_attribute.csv ``name`` prefix -> attribute key. The FoodOn attributes come
# numbered ("FoodOn Ontology ID #1 For FDC Item", "... #2 ...") and are kept as
# lists in that order.
_LIST_ATTRIBUTES = {
    "FoodOn Ontology ID": "foodon_terms",
    "FoodOn Ontology Name": "foodon_names",
}
_SCALAR_ATTRIBUTES = {
    "NCBI Taxon": "ncbi_taxon",
    "Scientific Name": "scientific_name",
}


def build_sr_legacy(
    sr_dir: Path, progress: ProgressCallback
) -> tuple[list[dict], pd.DataFrame]:
    """Food nodes and ``contains`` edges for every SR Legacy entry in *sr_dir*."""
    foods = read_foods(sr_dir)
    attributes = read_attributes(sr_dir, set(foods.index))
    nodes = [
        food_node(fdc_id, row, attributes.get(fdc_id, {}))
        for fdc_id, row in foods.iterrows()
    ]
    edges = read_nutrient_edges(sr_dir, set(foods.index), progress)
    logger.info(
        "FDC SR Legacy: %d foods, %d nutrient values from %s.",
        len(nodes),
        len(edges),
        sr_dir,
    )
    return nodes, edges


def read_foods(sr_dir: Path) -> pd.DataFrame:
    """SR Legacy foods indexed by fdc_id: description, NDB number, category."""
    legacy = pd.read_csv(sr_dir / "sr_legacy_food.csv", dtype=str)
    legacy["fdc_id"] = legacy["fdc_id"].astype(int)
    foods = pd.read_csv(
        sr_dir / "food.csv",
        usecols=["fdc_id", "description", "food_category_id"],
        dtype={"description": str, "food_category_id": str},
    )
    foods = foods[foods["fdc_id"].isin(legacy["fdc_id"])]
    categories = pd.read_csv(sr_dir / "food_category.csv", dtype=str).set_index("id")[
        "description"
    ]
    foods = foods.merge(legacy, on="fdc_id", how="inner").set_index("fdc_id")
    foods["category"] = foods["food_category_id"].map(categories).fillna("")
    missing = set(legacy["fdc_id"]) - set(foods.index)
    if missing:
        msg = f"{len(missing)} SR Legacy fdc_ids have no row in food.csv"
        raise ValueError(msg)
    return foods


def read_attributes(sr_dir: Path, fdc_ids: set[int]) -> dict[int, dict]:
    """USDA's FoodOn / NCBI mapping per food, as node attributes."""
    attrs = pd.read_csv(
        sr_dir / "food_attribute.csv",
        usecols=["fdc_id", "name", "value"],
        dtype={"name": str, "value": str},
    )
    attrs = attrs[attrs["fdc_id"].isin(fdc_ids)].dropna(subset=["name", "value"])
    result: dict[int, dict] = {}
    for fdc_id, group in attrs.sort_values("name").groupby("fdc_id"):
        result[int(fdc_id)] = _attributes_from_rows(group)
    return result


def _attributes_from_rows(rows: pd.DataFrame) -> dict:
    attrs: dict = {}
    for name, value in zip(rows["name"], rows["value"], strict=True):
        for prefix, key in _LIST_ATTRIBUTES.items():
            if name.startswith(prefix):
                attrs.setdefault(key, []).append(value.strip())
        if name in _SCALAR_ATTRIBUTES:
            attrs[_SCALAR_ATTRIBUTES[name]] = value.strip()
    return attrs


def food_node(fdc_id: int, row: pd.Series, attributes: dict) -> dict:
    name = str(row["description"]).strip().lower()
    return {
        "source_id": SOURCE_ID,
        "native_id": f"food:{fdc_id}",
        "name": name,
        "synonyms": [name],
        "synonym_types": ["label"],
        "node_type": "food",
        "raw_attrs": {
            "fdc_data_type": DATA_TYPE,
            "ndb_number": str(row["NDB_number"]),
            "fdc_category": str(row["category"]),
            **attributes,
        },
    }


def read_nutrient_edges(
    sr_dir: Path, fdc_ids: set[int], progress: ProgressCallback
) -> pd.DataFrame:
    """``contains`` edges for the SR Legacy rows of the (large) food_nutrient.csv."""
    chunks = pd.read_csv(
        sr_dir / "food_nutrient.csv",
        usecols=["fdc_id", "nutrient_id", "amount"],
        chunksize=_NUTRIENT_CHUNK_ROWS,
    )
    kept: list[pd.DataFrame] = []
    seen = 0
    for chunk in chunks:
        kept.append(chunk[chunk["fdc_id"].isin(fdc_ids)])
        seen += len(chunk)
        progress(seen, seen)
    values = pd.concat(kept, ignore_index=True)
    food_ids = values["fdc_id"].astype(int).astype(str)
    nutrient_ids = values["nutrient_id"].astype(int).astype(str)
    amounts = values["amount"].fillna(0.0).astype(float)
    return pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "head_native_id": "food:" + food_ids,
            "tail_native_id": "nutrient:" + nutrient_ids,
            "edge_type": "contains",
            "raw_attrs": [{"amount": a} for a in amounts],
        }
    )
