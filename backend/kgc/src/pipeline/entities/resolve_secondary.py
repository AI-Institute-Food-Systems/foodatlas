"""Pass 2 + 3: Link secondary sources and create unlinked entities.

FDC foods and nutrients live in :mod:`resolve_fdc`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.entity import ChemicalEntity
from .resolve_store_ops import _add_ext_id, _append_entities, _extract_chebi_int

if TYPE_CHECKING:
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore
    from .utils.lut import EntityLUT

logger = logging.getLogger(__name__)


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
