"""Resolve DMD molecules into the entity store."""

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


def resolve_dmd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    registry: EntityRegistry,
) -> None:
    """Resolve DMD molecules: enrich existing entities or create new ones.

    The registry is already seeded with DMD→entity_id mappings from the
    previous KG, so no Pass 2 linking is needed.
    """
    dmd = sources.get("dmd")
    if dmd is None:
        return
    nodes = dmd["nodes"]
    molecules = nodes[nodes["node_type"] == "molecule"]

    enriched = 0
    created_rows: list[dict] = []

    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        fa_id = registry.resolve("dmd", native)

        if fa_id and fa_id in store._entities.index:
            ext = store._entities.at[fa_id, "external_ids"]
            if "dmd" not in ext:
                ext["dmd"] = []
            if native not in ext["dmd"]:
                ext["dmd"].append(native)
            enriched += 1
            continue

        if not fa_id:
            fa_id = f"e{registry.next_eid}"
            registry.register("dmd", native, fa_id)

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
        "Pass 3: DMD — %d enriched, %d new entities.", enriched, len(created_rows)
    )
