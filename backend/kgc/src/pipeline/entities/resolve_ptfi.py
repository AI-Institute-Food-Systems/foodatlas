"""Resolve PTFI foods and chemicals across entity resolution passes 2 and 3.

PTFI is a secondary source: most of what it measures FoodAtlas already has, so
it links before it mints.

Pass 2 — link to existing entities via the adapter's xrefs: foods by FoodOn IRI
         (257 of 300 PTFI foods *are* an existing FoodAtlas food), chemicals by
         the first of their public ids that resolves — PubChem CID, then ChEBI,
         then FDC nutrient (:data:`CHEMICAL_KEYS` order).
Pass 3 — mint entities for the rest, registering their keys so the same food or
         chemical keeps its ``foodatlas_id`` on every later run.

That registration is the difference from the old merge script: it wrote
entities but not the registry, so PTFI ids were re-derived — and so moved —
every week.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd

from ...models.entity import ChemicalEntity, FoodEntity
from ..ingest.ptfi_keys import CHEMICAL_KEYS
from .resolve_ptfi_helpers import (
    add_ext_id,
    already_present,
    append_entities,
    chemical_external_ids,
    first_resolving,
    food_attributes,
    keys_by_native,
    registry_keys,
    resolve_or_mint,
)

if TYPE_CHECKING:
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore
    from .utils.lut import EntityLUT

logger = logging.getLogger(__name__)

_SAMPLE_IDS = "sample_ids"  # raw_attrs key holding a food group's GGB ids
_CHEMICAL_SOURCES = tuple(key.source for key in CHEMICAL_KEYS)


# -- Pass 2 ---------------------------------------------------------------
def link_ptfi_foods(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Link PTFI foods onto existing FoodAtlas foods sharing a FoodOn IRI."""
    xrefs = _xrefs(sources, ("foodon",))
    if xrefs.empty:
        return
    sample_ids = _attr_by_native(sources, "food", _SAMPLE_IDS)
    store_index = store._entities.index

    linked = 0
    for native_id, iri in zip(xrefs["native_id"], xrefs["target_id"], strict=False):
        fa_ids = [f for f in registry.resolve("foodon", iri) if f in store_index]
        if not fa_ids:
            continue
        for fa_id in fa_ids:
            for sample_id in sample_ids.get(native_id) or []:
                add_ext_id(store, fa_id, "ptfi", sample_id)
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
    """Link PTFI chemicals onto existing chemicals sharing any of their keys."""
    xrefs = _xrefs(sources, _CHEMICAL_SOURCES)
    if xrefs.empty:
        return
    ptfi_ids = _attr_by_native(sources, "chemical", "ptfi_id")
    store_index = store._entities.index

    linked = 0
    for native_id, keys in keys_by_native(xrefs).items():
        fa_ids = first_resolving(registry, keys, store_index)
        if not fa_ids:
            continue
        ptfi_id = ptfi_ids.get(native_id) or ""
        for fa_id in fa_ids:
            if ptfi_id:
                add_ext_id(store, fa_id, "ptfi", ptfi_id)
                registry.register_alias("ptfi", ptfi_id, fa_id)
        linked += 1
        linked_ids.add(native_id)
    logger.info("Pass 2: linked %d PTFI chemicals by public id.", linked)


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
        sample_ids = list(node["raw_attrs"].get(_SAMPLE_IDS, []))
        fa_id = resolve_or_mint(registry, [("ptfi", s) for s in sample_ids])
        if already_present(store, rows, fa_id, {"ptfi": sample_ids}):
            continue
        entity = FoodEntity(
            foodatlas_id=fa_id,
            common_name=node["name"],
            scientific_name=node["raw_attrs"].get("scientific_name", ""),
            synonyms=list(node["synonyms"]),
            external_ids={"ptfi": sample_ids},
            attributes=food_attributes(node),
        )
        rows[fa_id] = entity.model_dump(by_alias=True)
        if entity.common_name:
            lut.add("food", entity.common_name, entity.foodatlas_id)
    store._curr_eid = registry.next_eid
    append_entities(store, list(rows.values()))
    logger.info("Pass 3: %d unlinked PTFI food entities.", len(rows))


def create_unlinked_ptfi_chemicals(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    linked_ids: set[str],
    registry: EntityRegistry,
) -> None:
    """Create chemical entities for PTFI chemicals FoodAtlas did not have.

    Registered under every public id the chemical carries, so a chemical a
    later ontology refresh brings in under the same CID or ChEBI id resolves to
    this entity rather than a duplicate.
    """
    unlinked = _unlinked_nodes(sources, "chemical", linked_ids)
    rows: dict[str, dict] = {}
    for _, node in unlinked.iterrows():
        attrs = node["raw_attrs"]
        external_ids = chemical_external_ids(attrs)
        if not external_ids:
            continue
        fa_id = resolve_or_mint(registry, registry_keys(external_ids))
        if already_present(store, rows, fa_id, external_ids):
            continue
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
    append_entities(store, list(rows.values()))
    logger.info("Pass 3: %d unlinked PTFI chemical entities.", len(rows))


# -- helpers --------------------------------------------------------------
def _xrefs(
    sources: dict[str, dict[str, pd.DataFrame]], target_sources: tuple[str, ...]
) -> pd.DataFrame:
    ptfi = sources.get("ptfi")
    if ptfi is None:
        return pd.DataFrame()
    xrefs = ptfi.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return xrefs
    return xrefs[xrefs["target_source"].isin(target_sources)]


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


def _attr_by_native(
    sources: dict[str, dict[str, pd.DataFrame]], node_type: str, attr: str
) -> dict[str, Any]:
    """One ``raw_attrs`` value per node of *node_type*, by native id."""
    nodes = _nodes(sources)
    if nodes.empty:
        return {}
    of_type = nodes[nodes["node_type"] == node_type]
    return {
        native_id: attrs.get(attr)
        for native_id, attrs in zip(
            of_type["native_id"], of_type["raw_attrs"], strict=False
        )
    }
