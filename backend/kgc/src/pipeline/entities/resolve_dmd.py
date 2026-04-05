"""Resolve DMD molecules into the entity store.

Pass 2: enrich existing entities with DMD cross-references.
Pass 3: create new entities for unlinked DMD molecules.
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


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])


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
    """Pass 3: create new entities for DMD molecules not linked in Pass 2."""
    molecules = _get_molecules(sources)
    if molecules is None:
        return
    created_rows: list[dict] = []
    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        fa_id = registry.resolve("dmd", native)

        if fa_id and fa_id in store._entities.index:
            continue  # Already enriched in Pass 2.

        if not fa_id:
            fa_id = f"e{registry.next_eid}"
            registry.register("dmd", native, fa_id)
        else:
            # Stale registry entry → assign a fresh ID.
            fa_id = f"e{registry.next_eid}"
            registry.reassign("dmd", native, fa_id)

        composition = ""
        synonyms_raw = row.get("synonyms", [])
        if isinstance(synonyms_raw, list) and len(synonyms_raw) > 1:
            composition = synonyms_raw[1]

        syns = [row["name"]] if row["name"] else []
        if composition:
            syns.append(composition)

        entity = ChemicalEntity(
            foodatlas_id=fa_id,
            common_name=row["name"],
            synonyms=syns,
            external_ids={"dmd": [native]},
        )
        created_rows.append(entity.model_dump(by_alias=True))
        if entity.common_name:
            lut.add("chemical", entity.common_name, entity.foodatlas_id)

    store._curr_eid = registry.next_eid
    _append_entities(store, created_rows)
    logger.info("Pass 3: DMD — %d new entities.", len(created_rows))
