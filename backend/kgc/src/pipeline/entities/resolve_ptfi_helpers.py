"""Registry and store helpers for PTFI entity resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from ..ingest.ptfi_keys import CHEMICAL_KEYS

if TYPE_CHECKING:
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore

_SAMPLE_IDS = "sample_ids"  # raw_attrs key holding a food group's GGB ids
_CHEMICAL_SOURCES = tuple(key.source for key in CHEMICAL_KEYS)

RegistryKey = tuple[str, str]
"""(registry source, native id)."""


def keys_by_native(xrefs: pd.DataFrame) -> dict[str, list[RegistryKey]]:
    """Each chemical's registry keys, in ``CHEMICAL_KEYS`` priority order."""
    rank = {source: i for i, source in enumerate(_CHEMICAL_SOURCES)}
    ordered = xrefs.iloc[xrefs["target_source"].map(rank).argsort(kind="stable")]
    keys: dict[str, list[RegistryKey]] = {}
    for native_id, source, target_id in zip(
        ordered["native_id"],
        ordered["target_source"],
        ordered["target_id"],
        strict=False,
    ):
        keys.setdefault(native_id, []).append((source, str(target_id)))
    return keys


def first_resolving(
    registry: EntityRegistry, keys: list[RegistryKey], store_index: pd.Index
) -> list[str]:
    """Entities the first resolvable key maps to; [] when none does."""
    for source, native_id in keys:
        fa_ids = [f for f in registry.resolve(source, native_id) if f in store_index]
        if fa_ids:
            return fa_ids
    return []


def chemical_external_ids(attrs: dict) -> dict[str, list]:
    """The public ids a PTFI chemical node carries, as entity ``external_ids``."""
    return {
        key.external_ids_key: [int(attrs[key.column])]
        for key in CHEMICAL_KEYS
        if attrs.get(key.column)
    }


def registry_keys(external_ids: dict[str, list]) -> list[RegistryKey]:
    return [
        (key.source, str(value))
        for key in CHEMICAL_KEYS
        for value in external_ids.get(key.external_ids_key, [])
    ]


def resolve_or_mint(registry: EntityRegistry, keys: list[RegistryKey]) -> str:
    """Reuse the id any of *keys* already maps to, else mint one and register
    every key to it."""
    for source, native_id in keys:
        existing = registry.resolve(source, native_id)
        if existing:
            return existing[0]
    fa_id = f"e{registry.next_eid}"
    for source, native_id in keys:
        registry.register(source, native_id, fa_id)
    return fa_id


def already_present(
    store: EntityStore,
    rows: dict[str, dict],
    fa_id: str,
    external_ids: dict[str, list],
) -> bool:
    """True when *fa_id* is already an entity (or pending); records the xrefs.

    The registry can map several native ids onto one entity, so the same
    foodatlas_id can come up twice in one pass — append the external ids rather
    than building a duplicate row.
    """
    if fa_id in store._entities.index:
        for key, values in external_ids.items():
            for value in values:
                add_ext_id(store, fa_id, key, value)
        return True
    if fa_id in rows:
        for key, values in external_ids.items():
            existing = rows[fa_id]["external_ids"].setdefault(key, [])
            existing.extend(v for v in values if v not in existing)
        return True
    return False


def food_attributes(node: pd.Series) -> dict:
    attributes: dict = {"source": "ptfi"}
    sample_ids = node["raw_attrs"].get(_SAMPLE_IDS, [])
    if sample_ids:
        attributes[_SAMPLE_IDS] = list(sample_ids)
    food_groups = node["raw_attrs"].get("food_groups", [])
    if food_groups:
        attributes["food_groups"] = list(food_groups)
    return attributes


def add_ext_id(store: EntityStore, fa_id: str, key: str, value: object) -> None:
    """Append *value* to the entity's external_ids[key] if not already there."""
    external_ids = store._entities.at[fa_id, "external_ids"]
    if key not in external_ids:
        external_ids[key] = []
    if value not in external_ids[key]:
        external_ids[key].append(value)


def append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])
