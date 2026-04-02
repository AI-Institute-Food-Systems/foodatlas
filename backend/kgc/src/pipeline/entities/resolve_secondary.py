"""Pass 2 + 3: Link secondary sources and create unlinked entities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.entity import ChemicalEntity, FoodEntity

if TYPE_CHECKING:
    from ...config.corrections import Corrections
    from ...stores.entity_store import EntityStore
    from .lut import EntityLUT

logger = logging.getLogger(__name__)


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])


def _extract_chebi_int(chebi_ref: str) -> int:
    if "CHEBI_" in chebi_ref:
        return int(chebi_ref.rsplit("CHEBI_", maxsplit=1)[-1])
    return int(chebi_ref)


def _build_chebi_to_fa(store: EntityStore) -> dict[int, str]:
    result: dict[int, str] = {}
    for eid, row in store._entities.iterrows():
        for cid in row["external_ids"].get("chebi", []):
            result[int(cid)] = str(eid)
    return result


def _build_foodon_to_fa(store: EntityStore) -> dict[str, str]:
    result: dict[str, str] = {}
    for eid, row in store._entities.iterrows():
        for fid in row["external_ids"].get("foodon", []):
            result[str(fid)] = str(eid)
    return result


def _build_external_index(store: EntityStore, key: str) -> dict[str, str]:
    """Build a hash map from external_ids[key] values → entity ID."""
    result: dict[str, str] = {}
    for eid, row in store._entities.iterrows():
        for val in row["external_ids"].get(key, []):
            result[str(val)] = str(eid)
    return result


# -- Pass 2 ---------------------------------------------------------------


def link_cdno_to_chebi(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
) -> None:
    """Link CDNO entries to existing ChEBI entities via xrefs."""
    cdno = sources.get("cdno")
    if cdno is None:
        return
    xrefs = cdno.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return
    chebi_xrefs = xrefs[xrefs["target_source"] == "chebi"]
    chebi2fa = _build_chebi_to_fa(store)
    linked = 0
    for _, xref in chebi_xrefs.iterrows():
        chebi_id = _extract_chebi_int(xref["target_id"])
        if chebi_id in chebi2fa:
            fa_id = chebi2fa[chebi_id]
            ext = store._entities.at[fa_id, "external_ids"]
            if "cdno" not in ext:
                ext["cdno"] = []
            ext["cdno"].append(xref["native_id"])
            linked += 1
    logger.info("Pass 2: linked %d CDNO → ChEBI.", linked)


def link_fdc_foods_to_foodon(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    corrections: Corrections,
    linked_ids: set[str],
) -> None:
    """Link FDC food entries to existing FoodOn entities via xrefs."""
    fdc = sources.get("fdc")
    if fdc is None:
        return
    xrefs = fdc.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return
    foodon_xrefs = xrefs[xrefs["target_source"] == "foodon"]
    foodon2fa = _build_foodon_to_fa(store)
    linked = 0
    for _, xref in foodon_xrefs.iterrows():
        fdc_native = xref["native_id"]
        fdc_id = int(fdc_native.split(":")[-1])
        foodon_url = xref["target_id"]
        if fdc_id in corrections.fdc.food_overrides:
            foodon_url = corrections.fdc.food_overrides[fdc_id]
        if fdc_id in corrections.fdc.multi_foodon_resolution:
            foodon_url = corrections.fdc.multi_foodon_resolution[fdc_id]
        if foodon_url in foodon2fa:
            fa_id = foodon2fa[foodon_url]
            ext = store._entities.at[fa_id, "external_ids"]
            if "fdc" not in ext:
                ext["fdc"] = []
            ext["fdc"].append(fdc_id)
            linked += 1
            linked_ids.add(fdc_native)
    logger.info("Pass 2: linked %d FDC foods → FoodOn.", linked)


def link_fdc_nutrients(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    linked_ids: set[str],
) -> None:
    """Link FDC nutrients to existing entities via CDNO xrefs."""
    fdc = sources.get("fdc")
    cdno = sources.get("cdno")
    if fdc is None:
        return
    nodes = fdc["nodes"]
    nutrients = nodes[nodes["node_type"] == "nutrient"]
    cdno_xrefs = cdno.get("xrefs", pd.DataFrame()) if cdno else pd.DataFrame()
    fdc_nutrient_xrefs = (
        cdno_xrefs[cdno_xrefs["target_source"] == "fdc_nutrient"]
        if not cdno_xrefs.empty
        else pd.DataFrame()
    )

    fdc_to_cdno: dict[str, str] = {}
    for _, xref in fdc_nutrient_xrefs.iterrows():
        fdc_to_cdno[xref["target_id"]] = xref["native_id"]

    cdno_index = _build_external_index(store, "cdno")

    linked = 0
    for _, row in nutrients.iterrows():
        nutrient_id = row["native_id"].split(":")[-1]
        if nutrient_id in fdc_to_cdno:
            cdno_id = fdc_to_cdno[nutrient_id]
            fa_id = cdno_index.get(cdno_id)
            if fa_id:
                ext = store._entities.at[fa_id, "external_ids"]
                if "fdc_nutrient" not in ext:
                    ext["fdc_nutrient"] = []
                ext["fdc_nutrient"].append(int(nutrient_id))
                linked += 1
                linked_ids.add(row["native_id"])
    logger.info("Pass 2: linked %d FDC nutrients.", linked)


# -- Pass 3 ---------------------------------------------------------------


def create_unlinked_cdno(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
) -> None:
    """Create chemical entities for CDNO entries not linked to ChEBI."""
    cdno = sources.get("cdno")
    if cdno is None:
        return
    nodes = cdno["nodes"]
    linked_ids = _build_external_index(store, "cdno")

    unlinked = nodes[~nodes["native_id"].isin(linked_ids)]
    rows: list[dict] = []
    for _, row in unlinked.iterrows():
        entity = ChemicalEntity(
            foodatlas_id=f"e{store._curr_eid}",
            common_name=row["name"],
            synonyms=[row["name"]] if row["name"] else [],
            external_ids={"cdno": [row["native_id"]]},
        )
        rows.append(entity.model_dump(by_alias=True))
        if entity.common_name:
            lut.add("chemical", entity.common_name, entity.foodatlas_id)
        store._curr_eid += 1
    _append_entities(store, rows)
    logger.info("Pass 3: %d unlinked CDNO entities.", len(rows))


def create_unlinked_fdc_foods(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    linked_ids: set[str],
) -> None:
    """Create food entities for FDC foods not linked to FoodOn."""
    fdc = sources.get("fdc")
    if fdc is None:
        return
    nodes = fdc["nodes"]
    foods = nodes[nodes["node_type"] == "food"]
    unlinked = foods[~foods["native_id"].isin(linked_ids)]
    rows: list[dict] = []
    for _, row in unlinked.iterrows():
        fdc_id = int(row["native_id"].split(":")[-1])
        entity = FoodEntity(
            foodatlas_id=f"e{store._curr_eid}",
            common_name=row["name"],
            synonyms=[row["name"]] if row["name"] else [],
            external_ids={"fdc": [fdc_id]},
        )
        rows.append(entity.model_dump(by_alias=True))
        if entity.common_name:
            lut.add("food", entity.common_name, entity.foodatlas_id)
        store._curr_eid += 1
    _append_entities(store, rows)
    logger.info("Pass 3: %d unlinked FDC food entities.", len(rows))


def create_unlinked_fdc_nutrients(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    linked_ids: set[str],
) -> None:
    """Create chemical entities for FDC nutrients not linked elsewhere."""
    fdc = sources.get("fdc")
    if fdc is None:
        return
    nodes = fdc["nodes"]
    nutrients = nodes[nodes["node_type"] == "nutrient"]
    unlinked = nutrients[~nutrients["native_id"].isin(linked_ids)]
    rows: list[dict] = []
    for _, row in unlinked.iterrows():
        nutrient_id = int(row["native_id"].split(":")[-1])
        entity = ChemicalEntity(
            foodatlas_id=f"e{store._curr_eid}",
            common_name=row["name"],
            synonyms=[row["name"]] if row["name"] else [],
            external_ids={"fdc_nutrient": [nutrient_id]},
        )
        rows.append(entity.model_dump(by_alias=True))
        if entity.common_name:
            lut.add("chemical", entity.common_name, entity.foodatlas_id)
        store._curr_eid += 1
    _append_entities(store, rows)
    logger.info("Pass 3: %d unlinked FDC nutrient entities.", len(rows))
