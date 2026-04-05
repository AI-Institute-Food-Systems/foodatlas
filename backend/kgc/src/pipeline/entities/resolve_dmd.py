"""Resolve DMD molecules across all three entity resolution passes.

DMD is a primary source — each DMD ID maps 1-to-1 to one chemical.

Pass 1: Create entities for DMD molecules with seeded registry IDs.
Pass 2: Enrich existing entities (e.g. ChEBI) with DMD external IDs.
Pass 3: Create entities for genuinely new DMD molecules (no registry entry).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.entity import ChemicalEntity

if TYPE_CHECKING:
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore
    from .utils.lut import EntityLUT

logger = logging.getLogger(__name__)


def _get_molecules(
    sources: dict[str, dict[str, pd.DataFrame]],
) -> pd.DataFrame | None:
    dmd = sources.get("dmd")
    if dmd is None:
        return None
    nodes = dmd["nodes"]
    return nodes[nodes["node_type"] == "molecule"]


def _build_entity(native: str, row: pd.Series) -> dict[str, object]:
    composition = ""
    synonyms_raw = row.get("synonyms", [])
    if isinstance(synonyms_raw, list) and len(synonyms_raw) > 1:
        composition = synonyms_raw[1]

    syns = [row["name"]] if row["name"] else []
    if composition:
        syns.append(composition)

    entity = ChemicalEntity(
        foodatlas_id="",  # caller sets this
        common_name=row["name"],
        synonyms=syns,
        external_ids={"dmd": [native]},
    )
    result: dict[str, object] = entity.model_dump(by_alias=True)
    return result


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])


def create_chemicals_from_dmd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    registry: EntityRegistry,
) -> None:
    """Pass 1: create entities for DMD molecules with seeded registry IDs."""
    molecules = _get_molecules(sources)
    if molecules is None:
        return
    created_rows: list[dict] = []
    seen: set[str] = set()
    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        if native in seen:
            continue
        seen.add(native)
        fa_id = registry.resolve("dmd", native)
        if not fa_id or fa_id in store._entities.index:
            continue
        data = _build_entity(native, row)
        data["foodatlas_id"] = fa_id
        created_rows.append(data)
        if row["name"]:
            lut.add("chemical", row["name"], fa_id)

    store._curr_eid = registry.next_eid
    _append_entities(store, created_rows)
    logger.info("Pass 1: %d chemical entities from DMD.", len(created_rows))


def link_dmd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    registry: EntityRegistry,
) -> None:
    """Pass 2: enrich existing entities with DMD external IDs."""
    molecules = _get_molecules(sources)
    if molecules is None:
        return
    enriched = 0
    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        fa_id = registry.resolve("dmd", native)
        if not fa_id or fa_id not in store._entities.index:
            continue
        ext = store._entities.at[fa_id, "external_ids"]
        if "dmd" not in ext:
            ext["dmd"] = []
        if native not in ext["dmd"]:
            ext["dmd"].append(native)
        enriched += 1
    logger.info("Pass 2: DMD — %d entities enriched.", enriched)


def create_unlinked_dmd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    registry: EntityRegistry,
) -> None:
    """Pass 3: create entities for DMD molecules with no registry entry."""
    molecules = _get_molecules(sources)
    if molecules is None:
        return
    created_rows: list[dict] = []
    seen: set[str] = set()
    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        if native in seen:
            continue
        seen.add(native)
        if registry.resolve("dmd", native):
            continue  # Handled in Pass 1 or 2.
        fa_id = f"e{registry.next_eid}"
        registry.register("dmd", native, fa_id)
        data = _build_entity(native, row)
        data["foodatlas_id"] = fa_id
        created_rows.append(data)
        if row["name"]:
            lut.add("chemical", row["name"], fa_id)

    store._curr_eid = registry.next_eid
    _append_entities(store, created_rows)
    logger.info("Pass 3: %d unlinked DMD entities.", len(created_rows))
