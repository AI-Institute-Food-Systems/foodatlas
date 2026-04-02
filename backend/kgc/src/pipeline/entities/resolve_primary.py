"""Pass 1: Create primary entities from authoritative sources."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.entity import DiseaseEntity, FoodEntity

if TYPE_CHECKING:
    from ...config.corrections import Corrections
    from ...stores.entity_store import EntityStore
    from .lut import EntityLUT

logger = logging.getLogger(__name__)

_SYNONYM_PRIORITY: dict[str, int] = {
    "name": 10,
    "label": 9,
    "label_alt": 7,
    "exact": 6,
    "iupac": 5,
    "synonym": 4,
    "narrow": 3,
    "broad": 2,
    "taxon": 1,
}


def pick_common_name(
    synonyms: list[str],
    synonym_types: list[str],
    star: int = 0,
) -> str:
    """Select best common name based on type priority and star rating."""
    if not synonyms:
        return ""
    best_name = synonyms[0]
    best_score = -1
    for name, stype in zip(synonyms, synonym_types, strict=True):
        score = _SYNONYM_PRIORITY.get(stype, 0) + star
        if score > best_score:
            best_score = score
            best_name = name
    return best_name


def _get_list(row: pd.Series, key: str) -> list:
    val = row.get(key)
    if val is None:
        return []
    if isinstance(val, list):
        return val
    # Parquet returns list columns as numpy arrays
    return list(val)


def _get_star(row: pd.Series) -> int:
    attrs = row.get("raw_attrs")
    if isinstance(attrs, dict):
        return int(attrs.get("star", 0))
    return 0


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])


def create_foods_from_foodon(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
) -> None:
    """Create food entities from FoodOn nodes."""
    foodon = sources.get("foodon")
    if foodon is None:
        return
    nodes = foodon["nodes"]
    if "is_food" in nodes.columns:
        nodes = nodes[nodes["is_food"]]

    rows: list[dict] = []
    for _, row in nodes.iterrows():
        syns = _get_list(row, "synonyms")
        syn_types = _get_list(row, "synonym_types")
        name = pick_common_name(syns, syn_types)
        entity = FoodEntity(
            foodatlas_id=f"e{store._curr_eid}",
            common_name=name,
            synonyms=[s.lower() for s in syns],
            external_ids={"foodon": [row["native_id"]]},
        )
        rows.append(entity.model_dump(by_alias=True))
        for s in entity.synonyms:
            lut.add("food", s, entity.foodatlas_id)
        store._curr_eid += 1

    _append_entities(store, rows)
    logger.info("Pass 1: %d food entities from FoodOn.", len(rows))


def create_chemicals_from_chebi(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    corrections: Corrections,
) -> None:
    """Create chemical entities from ChEBI nodes (vectorized)."""
    chebi = sources.get("chebi")
    if chebi is None:
        return
    nodes = chebi["nodes"]
    drop_names = set(corrections.chebi_lut.drop_names)
    n = len(nodes)
    start_eid = store._curr_eid

    fa_ids = [f"e{start_eid + i}" for i in range(n)]
    store._curr_eid += n

    rows: list[dict] = []
    for i, (_, row) in enumerate(nodes.iterrows()):
        syns = _get_list(row, "synonyms")
        syn_types = _get_list(row, "synonym_types")
        star = _get_star(row)
        name = pick_common_name(syns, syn_types, star)
        filtered = [s.lower() for s in syns if s.lower() not in drop_names]
        fa_id = fa_ids[i]
        rows.append(
            {
                "foodatlas_id": fa_id,
                "entity_type": "chemical",
                "common_name": name,
                "scientific_name": "",
                "synonyms": filtered,
                "external_ids": {"chebi": [int(row["native_id"])]},
            }
        )
        for s in filtered:
            lut.add("chemical", s, fa_id)

    _append_entities(store, rows)
    logger.info("Pass 1: %d chemical entities from ChEBI.", len(rows))


def create_diseases_from_ctd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
) -> None:
    """Create disease entities from CTD nodes."""
    ctd = sources.get("ctd")
    if ctd is None:
        return
    nodes = ctd["nodes"]

    rows: list[dict] = []
    for _, row in nodes.iterrows():
        syns = _get_list(row, "synonyms")
        name = row["name"] if row["name"] else (syns[0] if syns else "")
        entity = DiseaseEntity(
            foodatlas_id=f"e{store._curr_eid}",
            common_name=name.lower(),
            synonyms=[s.lower() for s in syns],
            external_ids={"ctd": [row["native_id"]]},
        )
        rows.append(entity.model_dump(by_alias=True))
        for s in entity.synonyms:
            lut.add("disease", s, entity.foodatlas_id)
        store._curr_eid += 1

    _append_entities(store, rows)
    logger.info("Pass 1: %d disease entities from CTD.", len(rows))
