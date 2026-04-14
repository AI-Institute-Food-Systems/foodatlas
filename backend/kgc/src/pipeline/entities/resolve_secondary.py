"""Pass 2 + 3: Link secondary sources and create unlinked entities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.entity import ChemicalEntity, FoodEntity

if TYPE_CHECKING:
    from ...config.corrections import Corrections
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore
    from .utils.lut import EntityLUT

logger = logging.getLogger(__name__)


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])


def _extract_chebi_int(chebi_ref: str) -> int:
    if "CHEBI_" in chebi_ref:
        return int(chebi_ref.rsplit("CHEBI_", maxsplit=1)[-1])
    return int(chebi_ref)


def _add_ext_id(store: EntityStore, fa_id: str, key: str, value: object) -> None:
    """Append *value* to entity's external_ids[key] if not present."""
    ext = store._entities.at[fa_id, "external_ids"]
    if key not in ext:
        ext[key] = []
    if value not in ext[key]:
        ext[key].append(value)


# -- Pass 2 ---------------------------------------------------------------


def link_cdno_to_chebi(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    registry: EntityRegistry,
) -> None:
    """Link CDNO entries to existing ChEBI entities via xrefs."""
    cdno = sources.get("cdno")
    if cdno is None:
        return
    xrefs = cdno.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return
    chebi_xrefs = xrefs[xrefs["target_source"] == "chebi"]
    store_index = store._entities.index
    linked = 0
    for _, xref in chebi_xrefs.iterrows():
        chebi_id = _extract_chebi_int(xref["target_id"])
        fa_ids = [
            f for f in registry.resolve("chebi", str(chebi_id)) if f in store_index
        ]
        for fa_id in fa_ids:
            _add_ext_id(store, fa_id, "cdno", xref["native_id"])
            registry.register_alias("cdno", str(xref["native_id"]), fa_id)
        if fa_ids:
            linked += 1
    logger.info("Pass 2: linked %d CDNO → ChEBI.", linked)


def link_fdc_foods_to_foodon(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    corrections: Corrections,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Link FDC food entries to existing FoodOn entities via xrefs."""
    fdc = sources.get("fdc")
    if fdc is None:
        return
    xrefs = fdc.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return
    foodon_xrefs = xrefs[xrefs["target_source"] == "foodon"]
    store_index = store._entities.index
    linked = 0
    for _, xref in foodon_xrefs.iterrows():
        fdc_native = xref["native_id"]
        fdc_id = int(fdc_native.split(":")[-1])
        foodon_url = xref["target_id"]
        if fdc_id in corrections.fdc.food_overrides:
            foodon_url = corrections.fdc.food_overrides[fdc_id]
        if fdc_id in corrections.fdc.multi_foodon_resolution:
            foodon_url = corrections.fdc.multi_foodon_resolution[fdc_id]
        fa_ids = [f for f in registry.resolve("foodon", foodon_url) if f in store_index]
        for fa_id in fa_ids:
            _add_ext_id(store, fa_id, "fdc", fdc_id)
            registry.register_alias("fdc", str(fdc_id), fa_id)
        if fa_ids:
            linked += 1
            linked_ids.add(fdc_native)
    logger.info("Pass 2: linked %d FDC foods → FoodOn.", linked)


def link_fdc_nutrients(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    linked_ids: set[str],
    registry: EntityRegistry,
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

    fdc_to_cdno: dict[str, list[str]] = {}
    for _, xref in fdc_nutrient_xrefs.iterrows():
        fdc_to_cdno.setdefault(xref["target_id"], []).append(xref["native_id"])

    store_index = store._entities.index
    linked = 0
    for _, row in nutrients.iterrows():
        nutrient_id = row["native_id"].split(":")[-1]
        cdno_ids = fdc_to_cdno.get(nutrient_id, [])
        if not cdno_ids:
            continue
        # Resolve CDNO IDs via registry to find matching entities.
        fa_id_set: set[str] = set()
        for c in cdno_ids:
            fa_id_set.update(f for f in registry.resolve("cdno", c) if f in store_index)
        if not fa_id_set:
            continue
        nid = int(nutrient_id)
        for fa_id in fa_id_set:
            _add_ext_id(store, fa_id, "fdc_nutrient", nid)
            registry.register_alias("fdc_nutrient", str(nutrient_id), fa_id)
        linked += 1
        linked_ids.add(row["native_id"])
    logger.info("Pass 2: linked %d FDC nutrients.", linked)


# -- Pass 3 ---------------------------------------------------------------


def create_unlinked_cdno(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    registry: EntityRegistry,
) -> None:
    """Create chemical entities for CDNO entries not linked to ChEBI."""
    cdno = sources.get("cdno")
    if cdno is None:
        return
    nodes = cdno["nodes"]
    # A CDNO ID is "linked" if the registry has it.
    linked_natives = {
        native
        for _, row in nodes.iterrows()
        if registry.resolve("cdno", (native := str(row["native_id"])))
    }

    unlinked = nodes[~nodes["native_id"].isin(linked_natives)]
    rows_by_id: dict[str, dict] = {}
    for _, row in unlinked.iterrows():
        native = str(row["native_id"])
        fa_ids = registry.resolve("cdno", native)
        if not fa_ids:
            fa_id = f"e{registry.next_eid}"
            registry.register("cdno", native, fa_id)
        else:
            fa_id = fa_ids[0]

        if fa_id in store._entities.index:
            _add_ext_id(store, fa_id, "cdno", native)
            continue
        if fa_id in rows_by_id:
            ext = rows_by_id[fa_id]["external_ids"]
            if native not in ext.get("cdno", []):
                ext.setdefault("cdno", []).append(native)
            continue

        entity = ChemicalEntity(
            foodatlas_id=fa_id,
            common_name=row["name"],
            synonyms=[row["name"]] if row["name"] else [],
            external_ids={"cdno": [row["native_id"]]},
        )
        rows_by_id[fa_id] = entity.model_dump(by_alias=True)
        if entity.common_name:
            lut.add("chemical", entity.common_name, entity.foodatlas_id)
    store._curr_eid = registry.next_eid
    _append_entities(store, list(rows_by_id.values()))
    logger.info("Pass 3: %d unlinked CDNO entities.", len(rows_by_id))


def create_unlinked_fdc_foods(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Create food entities for FDC foods not linked to FoodOn."""
    fdc = sources.get("fdc")
    if fdc is None:
        return
    nodes = fdc["nodes"]
    foods = nodes[nodes["node_type"] == "food"]
    unlinked = foods[~foods["native_id"].isin(linked_ids)]
    rows_by_id: dict[str, dict] = {}
    for _, row in unlinked.iterrows():
        fdc_id = int(row["native_id"].split(":")[-1])
        native = str(fdc_id)
        fa_ids = registry.resolve("fdc", native)
        if not fa_ids:
            fa_id = f"e{registry.next_eid}"
            registry.register("fdc", native, fa_id)
        else:
            fa_id = fa_ids[0]

        if fa_id in store._entities.index:
            _add_ext_id(store, fa_id, "fdc", fdc_id)
            continue
        if fa_id in rows_by_id:
            ext = rows_by_id[fa_id]["external_ids"]
            if fdc_id not in ext.get("fdc", []):
                ext.setdefault("fdc", []).append(fdc_id)
            continue

        entity = FoodEntity(
            foodatlas_id=fa_id,
            common_name=row["name"],
            synonyms=[row["name"]] if row["name"] else [],
            external_ids={"fdc": [fdc_id]},
        )
        rows_by_id[fa_id] = entity.model_dump(by_alias=True)
        if entity.common_name:
            lut.add("food", entity.common_name, entity.foodatlas_id)
    store._curr_eid = registry.next_eid
    _append_entities(store, list(rows_by_id.values()))
    logger.info("Pass 3: %d unlinked FDC food entities.", len(rows_by_id))


def create_unlinked_fdc_nutrients(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Create chemical entities for FDC nutrients not linked elsewhere."""
    fdc = sources.get("fdc")
    if fdc is None:
        return
    nodes = fdc["nodes"]
    nutrients = nodes[nodes["node_type"] == "nutrient"]
    unlinked = nutrients[~nutrients["native_id"].isin(linked_ids)]
    rows_by_id: dict[str, dict] = {}
    for _, row in unlinked.iterrows():
        nutrient_id = int(row["native_id"].split(":")[-1])
        native = str(nutrient_id)
        fa_ids = registry.resolve("fdc_nutrient", native)
        if not fa_ids:
            fa_id = f"e{registry.next_eid}"
            registry.register("fdc_nutrient", native, fa_id)
        else:
            fa_id = fa_ids[0]

        if fa_id in store._entities.index:
            _add_ext_id(store, fa_id, "fdc_nutrient", nutrient_id)
            continue
        if fa_id in rows_by_id:
            ext = rows_by_id[fa_id]["external_ids"]
            if nutrient_id not in ext.get("fdc_nutrient", []):
                ext.setdefault("fdc_nutrient", []).append(nutrient_id)
            continue

        entity = ChemicalEntity(
            foodatlas_id=fa_id,
            common_name=row["name"],
            synonyms=[row["name"]] if row["name"] else [],
            external_ids={"fdc_nutrient": [nutrient_id]},
        )
        rows_by_id[fa_id] = entity.model_dump(by_alias=True)
        if entity.common_name:
            lut.add("chemical", entity.common_name, entity.foodatlas_id)
    store._curr_eid = registry.next_eid
    _append_entities(store, list(rows_by_id.values()))
    logger.info("Pass 3: %d unlinked FDC nutrient entities.", len(rows_by_id))
