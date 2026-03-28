"""Process and clean FoodOn ontology data."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from owlready2 import get_ontology

from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def process_foodon(settings: KGCSettings) -> None:
    """Clean raw FoodOn data and save to data_cleaning output dir."""
    data_dir = Path(settings.data_dir)
    dp_dir = Path(settings.data_cleaning_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    foodon_synonyms = pd.read_csv(data_dir / "FoodOn" / "foodon-synonyms.tsv", sep="\t")
    foodon = _clean(foodon_synonyms)
    foodon = _label_is_food(foodon)
    foodon = _label_is_organism(foodon)
    foodon = foodon[foodon["is_food"] | foodon["is_organism"]]
    foodon = _append_additional_relationships(foodon, data_dir)
    foodon.to_parquet(dp_dir / "foodon_cleaned.parquet")
    logger.info("Processed FoodOn: %d entries.", len(foodon))


def _clean(foodon_synonyms: pd.DataFrame) -> pd.DataFrame:
    """Parse raw FoodOn synonym dump into structured entities."""

    def _remove_brackets(x: object) -> object:
        if pd.isna(x):
            return x
        s = str(x)
        if s.startswith("<") and s.endswith(">"):
            return s[1:-1]
        return s

    def _remove_suffix(x: str) -> str:
        for sep in ("@", "^^"):
            if sep in x:
                return x.split(sep, maxsplit=1)[0]
        return x

    def _parse_entity(group: pd.DataFrame) -> dict:
        parents = group["?parent"].dropna().tolist()
        synonyms: dict[str, list[str]] = {
            "label": [],
            "label (alternative)": [],
            "synonym (exact)": [],
            "synonym": [],
            "synonym (narrow)": [],
            "synonym (broad)": [],
            "taxon": [],
        }
        for _, row in group.dropna(subset=["?type"]).iterrows():
            synonyms[row["?type"]].append(_remove_suffix(row["?label"]))
        return {"parents": parents, "synonyms": synonyms}

    foodon_synonyms["?class"] = foodon_synonyms["?class"].apply(_remove_brackets)
    foodon_synonyms["?parent"] = foodon_synonyms["?parent"].apply(_remove_brackets)
    entities = foodon_synonyms.groupby("?class").apply(_parse_entity).apply(pd.Series)
    entities.index.name = "foodon_id"
    return entities


def _label_is_food(foodon: pd.DataFrame) -> pd.DataFrame:
    """DFS to label entries descending from 'food product by organism'."""
    root = "http://www.w3.org/2002/07/owl#Thing"
    food = "http://purl.obolibrary.org/obo/FOODON_00002381"
    visited: dict[str, bool] = {food: True}

    def dfs(foodon_id: str) -> bool:
        if foodon_id in visited:
            return visited[foodon_id]
        if foodon_id not in foodon.index or foodon_id == root:
            return False
        result = any(dfs(p) for p in foodon.loc[foodon_id, "parents"])
        visited[foodon_id] = result
        return result

    for fid in foodon.index:
        dfs(fid)
    foodon["is_food"] = foodon.index.map(visited)
    return foodon


def _label_is_organism(foodon: pd.DataFrame) -> pd.DataFrame:
    """DFS to label entries descending from 'organism'."""
    root = "http://www.w3.org/2002/07/owl#Thing"
    organism = "http://purl.obolibrary.org/obo/OBI_0100026"
    visited: dict[str, bool] = {organism: True}

    def dfs(foodon_id: str) -> bool:
        if foodon_id in visited:
            return visited[foodon_id]
        if foodon_id not in foodon.index or foodon_id == root:
            return False
        result = any(dfs(p) for p in foodon.loc[foodon_id, "parents"])
        visited[foodon_id] = result
        return result

    for fid in foodon.index:
        dfs(fid)
    foodon["is_organism"] = foodon.index.map(visited)
    return foodon


def _append_additional_relationships(
    foodon: pd.DataFrame, data_dir: Path
) -> pd.DataFrame:
    """Derive derives_from, in_taxon, derives, has_part relationships."""
    onto = get_ontology(str(data_dir / "FoodOn" / "foodon.owl")).load()
    obo = onto.get_namespace("http://purl.obolibrary.org/obo/")

    results = foodon.apply(lambda row: _parse_derives_from(row, obo), axis=1).apply(
        pd.Series
    )
    foodon[["derives_from", "in_taxon"]] = results

    derives: dict[str, list[str]] = {}
    for fid, row in foodon.iterrows():
        if not row["is_food"]:
            continue
        for e in row["derives_from"]:
            if e not in derives:
                derives[e] = []
            derives[e].append(str(fid))
    foodon["derives"] = foodon.index.map(lambda x: derives.get(x, []))

    has_part: dict[str, list[str]] = {}
    for fid, row in foodon.iterrows():
        if not row["is_food"]:
            continue
        for e in row["in_taxon"]:
            if e not in has_part:
                has_part[e] = []
            has_part[e].append(str(fid))
    foodon["has_part"] = foodon.index.map(lambda x: has_part.get(x, []))

    return foodon


def _parse_derives_from(
    row: pd.Series,
    obo: Any,
) -> dict[str, list[str]]:
    """Extract derives_from and in_taxon from OWL relationships."""
    obo_ns: Any = obo
    result: dict[str, list[str]] = {"derives_from": [], "in_taxon": []}
    foodon_id = row.name.split("/")[-1]
    entity = obo_ns[str(foodon_id)]
    if entity is None:
        return result

    derives_prop = obo_ns["RO_0001000"]
    taxon_prop = obo_ns["RO_0002162"]

    for rel in entity.is_a:
        if not hasattr(rel, "property"):
            continue
        if rel.property not in (derives_prop, taxon_prop):
            continue
        values = [str(rel.value)]
        if " | " in values[0]:
            values = values[0].split(" | ")
        if " " in str(rel.value):
            continue
        renamed = [_rename_foodon_id(v) for v in values]
        if rel.property == derives_prop:
            result["derives_from"].extend(renamed)
        else:
            result["in_taxon"].extend(renamed)

    return result


def _rename_foodon_id(foodon_id: str) -> str:
    if foodon_id.startswith("obo."):
        return f"http://purl.obolibrary.org/obo/{foodon_id[4:]}"
    msg = f"Unknown foodon_id: {foodon_id}"
    raise ValueError(msg)
