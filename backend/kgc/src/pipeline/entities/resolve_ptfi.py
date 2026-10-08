"""Resolve PTFI foods and chemicals across entity resolution passes 2 and 3.

PTFI is a secondary source: most of what it measures FoodAtlas already has, so
it links before it mints.

Pass 2 — link to existing entities via the adapter's xrefs: foods by FoodOn IRI
         (the dedup that used to be hand-rolled in ``merge_ptfi_delta.py`` —
         257 of 300 PTFI foods *are* an existing FoodAtlas food), chemicals by
         PubChem CID.
Pass 3 — mint entities for the rest, registering their PTFI native ids so the
         same food or chemical keeps its ``foodatlas_id`` on every later run.

That registration is the difference from the merge script: it wrote entities but
not the registry, so PTFI ids were re-derived — and so moved — every week.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.entity import ChemicalEntity, FoodEntity

if TYPE_CHECKING:
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore
    from .utils.lut import EntityLUT

logger = logging.getLogger(__name__)

_SAMPLE_IDS = "sample_ids"  # raw_attrs key holding a food group's GGB ids


# -- Pass 2 ---------------------------------------------------------------
def link_ptfi_foods(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Link PTFI foods onto existing FoodAtlas foods sharing a FoodOn IRI."""
    xrefs = _xrefs(sources, "foodon")
    if xrefs.empty:
        return
    sample_ids = _sample_ids_by_native(sources)
    store_index = store._entities.index

    linked = 0
    for native_id, iri in zip(xrefs["native_id"], xrefs["target_id"], strict=False):
        fa_ids = [f for f in registry.resolve("foodon", iri) if f in store_index]
        if not fa_ids:
            continue
        for fa_id in fa_ids:
            for sample_id in sample_ids.get(native_id, []):
                _add_ext_id(store, fa_id, "ptfi", sample_id)
                registry.register_alias("ptfi", sample_id, fa_id)
        linked += 1
        linked_ids.add(native_id)
    logger.info("Pass 2: linked %d PTFI foods → FoodOn.", linked)


def link_ptfi_chemicals(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Link PTFI chemicals onto existing FoodAtlas chemicals sharing a CID."""
    xrefs = _xrefs(sources, "pubchem")
    if xrefs.empty:
        return
    ptfi_ids = _ptfi_ids_by_native(sources)
    store_index = store._entities.index

    linked = 0
    for native_id, cid in zip(xrefs["native_id"], xrefs["target_id"], strict=False):
        fa_ids = [f for f in registry.resolve("pubchem", str(cid)) if f in store_index]
        if not fa_ids:
            continue
        for fa_id in fa_ids:
            ptfi_id = ptfi_ids.get(native_id, "")
            if ptfi_id:
                _add_ext_id(store, fa_id, "ptfi", ptfi_id)
                registry.register_alias("ptfi", ptfi_id, fa_id)
        linked += 1
        linked_ids.add(native_id)
    logger.info("Pass 2: linked %d PTFI chemicals → PubChem.", linked)


# -- Pass 3 ---------------------------------------------------------------
def create_unlinked_ptfi_foods(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Create food entities for PTFI foods that matched no existing food.

    These are PTFI samples with no FoodOn IRI — mostly prepared dishes, which
    FoodOn has no term for. Their GGB sample ids are the only stable key, so
    those are what the registry remembers.
    """
    unlinked = _unlinked_nodes(sources, "food", linked_ids)
    rows: dict[str, dict] = {}
    for _, node in unlinked.iterrows():
        sample_ids = node["raw_attrs"].get(_SAMPLE_IDS, [])
        fa_id = _resolve_or_mint(registry, "ptfi", sample_ids)
        if _already_present(store, rows, fa_id, "ptfi", sample_ids):
            continue
        entity = FoodEntity(
            foodatlas_id=fa_id,
            common_name=node["name"],
            scientific_name=node["raw_attrs"].get("scientific_name", ""),
            synonyms=list(node["synonyms"]),
            external_ids={"ptfi": list(sample_ids)},
            attributes=_food_attributes(node),
        )
        rows[fa_id] = entity.model_dump(by_alias=True)
        if entity.common_name:
            lut.add("food", entity.common_name, entity.foodatlas_id)
    store._curr_eid = registry.next_eid
    _append_entities(store, list(rows.values()))
    logger.info("Pass 3: %d unlinked PTFI food entities.", len(rows))


def create_unlinked_ptfi_chemicals(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Create chemical entities for PTFI chemicals FoodAtlas did not have.

    Keyed on the PubChem CID, so a chemical a later ontology refresh brings in
    under the same CID resolves to this entity rather than a duplicate.
    """
    unlinked = _unlinked_nodes(sources, "chemical", linked_ids)
    rows: dict[str, dict] = {}
    for _, node in unlinked.iterrows():
        attrs = node["raw_attrs"]
        cid = str(attrs.get("cid", ""))
        if not cid:
            continue
        fa_id = _resolve_or_mint(registry, "pubchem", [cid])
        if _already_present(store, rows, fa_id, "pubchem_compound", [cid]):
            continue
        external_ids: dict[str, list] = {"pubchem_compound": [int(cid)]}
        if attrs.get("ptfi_id"):
            external_ids["ptfi"] = [attrs["ptfi_id"]]
            registry.register_alias("ptfi", attrs["ptfi_id"], fa_id)
        if attrs.get("inchikey"):
            external_ids["inchikey"] = [attrs["inchikey"]]
        entity = ChemicalEntity(
            foodatlas_id=fa_id,
            common_name=node["name"],
            synonyms=list(node["synonyms"]),
            external_ids=external_ids,
            attributes={"source": "ptfi"},
        )
        rows[fa_id] = entity.model_dump(by_alias=True)
        if entity.common_name:
            lut.add("chemical", entity.common_name, entity.foodatlas_id)
    store._curr_eid = registry.next_eid
    _append_entities(store, list(rows.values()))
    logger.info("Pass 3: %d unlinked PTFI chemical entities.", len(rows))


# -- helpers --------------------------------------------------------------
def _xrefs(
    sources: dict[str, dict[str, pd.DataFrame]], target_source: str
) -> pd.DataFrame:
    ptfi = sources.get("ptfi")
    if ptfi is None:
        return pd.DataFrame()
    xrefs = ptfi.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return xrefs
    return xrefs[xrefs["target_source"] == target_source]


def _nodes(sources: dict[str, dict[str, pd.DataFrame]]) -> pd.DataFrame:
    ptfi = sources.get("ptfi")
    return pd.DataFrame() if ptfi is None else ptfi.get("nodes", pd.DataFrame())


def _unlinked_nodes(
    sources: dict[str, dict[str, pd.DataFrame]], node_type: str, linked_ids: set[str]
) -> pd.DataFrame:
    nodes = _nodes(sources)
    if nodes.empty:
        return nodes
    of_type = nodes[nodes["node_type"] == node_type]
    return of_type[~of_type["native_id"].isin(linked_ids)]


def _sample_ids_by_native(
    sources: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, list[str]]:
    nodes = _nodes(sources)
    if nodes.empty:
        return {}
    foods = nodes[nodes["node_type"] == "food"]
    return {
        native_id: attrs.get(_SAMPLE_IDS, [])
        for native_id, attrs in zip(
            foods["native_id"], foods["raw_attrs"], strict=False
        )
    }


def _ptfi_ids_by_native(
    sources: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, str]:
    nodes = _nodes(sources)
    if nodes.empty:
        return {}
    chemicals = nodes[nodes["node_type"] == "chemical"]
    return {
        native_id: attrs.get("ptfi_id", "")
        for native_id, attrs in zip(
            chemicals["native_id"], chemicals["raw_attrs"], strict=False
        )
    }


def _resolve_or_mint(
    registry: EntityRegistry, source: str, native_ids: list[str]
) -> str:
    """Reuse the id any of *native_ids* already maps to, else mint a new one."""
    for native_id in native_ids:
        existing = registry.resolve(source, str(native_id))
        if existing:
            return existing[0]
    fa_id = f"e{registry.next_eid}"
    for native_id in native_ids:
        registry.register(source, str(native_id), fa_id)
    return fa_id


def _already_present(
    store: EntityStore,
    rows: dict[str, dict],
    fa_id: str,
    ext_key: str,
    values: list,
) -> bool:
    """True when *fa_id* is already an entity (or pending); records the xref.

    The registry can map several native ids onto one entity, so the same
    foodatlas_id can come up twice in one pass — append the external id rather
    than building a duplicate row.
    """
    if fa_id in store._entities.index:
        for value in values:
            _add_ext_id(store, fa_id, ext_key, value)
        return True
    if fa_id in rows:
        existing = rows[fa_id]["external_ids"].setdefault(ext_key, [])
        existing.extend(v for v in values if v not in existing)
        return True
    return False


def _food_attributes(node: pd.Series) -> dict:
    attributes: dict = {"source": "ptfi"}
    sample_ids = node["raw_attrs"].get(_SAMPLE_IDS, [])
    if sample_ids:
        attributes[_SAMPLE_IDS] = list(sample_ids)
    food_groups = node["raw_attrs"].get("food_groups", [])
    if food_groups:
        attributes["food_groups"] = list(food_groups)
    return attributes


def _add_ext_id(store: EntityStore, fa_id: str, key: str, value: object) -> None:
    """Append *value* to the entity's external_ids[key] if not already there."""
    external_ids = store._entities.at[fa_id, "external_ids"]
    if key not in external_ids:
        external_ids[key] = []
    if value not in external_ids[key]:
        external_ids[key].append(value)


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])
