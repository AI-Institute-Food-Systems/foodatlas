"""Pass 1: Create chemical entities from DMD molecules.

DMD is a primary source — each DMD ID maps 1-to-1 to one chemical.
Molecules that already exist in the store (created by ChEBI) are
enriched with the DMD external ID; the rest are created as new entities.
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
    """Create or enrich chemical entities from DMD molecules.

    For each molecule:
    - If the registry points to an entity already in the store (e.g. a
      ChEBI chemical), enrich it with the DMD external ID.
    - Otherwise, create a new entity (reusing the seeded ID if available).
    """
    dmd = sources.get("dmd")
    if dmd is None:
        return
    nodes = dmd["nodes"]
    molecules = nodes[nodes["node_type"] == "molecule"]

    enriched = 0
    reused = 0
    new = 0
    created_rows: list[dict] = []
    seen: set[str] = set()

    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        if native in seen:
            continue
        seen.add(native)
        fa_id = registry.resolve("dmd", native)

        # Already in store (created by ChEBI) → enrich only.
        if fa_id and fa_id in store._entities.index:
            ext = store._entities.at[fa_id, "external_ids"]
            if "dmd" not in ext:
                ext["dmd"] = []
            if native not in ext["dmd"]:
                ext["dmd"].append(native)
            enriched += 1
            continue

        # Reuse seeded ID or assign a fresh one.
        if fa_id:
            reused += 1
        else:
            fa_id = f"e{registry.next_eid}"
            registry.register("dmd", native, fa_id)
            new += 1

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
    logger.info(
        "Pass 1: DMD — %d enriched, %d reused IDs, %d new.",
        enriched,
        reused,
        new,
    )
