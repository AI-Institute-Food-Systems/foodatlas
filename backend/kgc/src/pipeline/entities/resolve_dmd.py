"""Resolve DMD molecules across all three entity resolution passes.

Pass 1: Create entities for DMD molecules with seeded registry IDs.
Pass 2: Link DMD molecules to existing entities via ChEBI/PubChem xrefs,
        adding DMD native IDs to their external_ids.
Pass 3: Create entities for unlinked DMD molecules, disambiguating
        duplicate names with an xref-ID suffix.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .resolve_dmd_helpers import (
    _add_dmd_to_entity,
    _append_entities,
    _build_entity,
    _build_ext_index,
    _collect_unlinked,
    _get_molecules,
    _get_xrefs,
    _pick_display_xref,
)

if TYPE_CHECKING:
    import pandas as pd

    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore
    from .utils.lut import EntityLUT

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pass 1 — Create entities for DMD molecules with seeded registry IDs
# ---------------------------------------------------------------------------


def create_chemicals_from_dmd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    registry: EntityRegistry,
) -> None:
    """Pass 1: create entities for seeded DMD molecules already in the store.

    Only creates entities whose seeded ``fa_id`` is already in the store
    (i.e. created by a primary source like ChEBI). DMD-only seeded
    molecules are left for Pass 2 (xref linking) or Pass 3 (new entity).
    """
    molecules = _get_molecules(sources)
    if molecules is None:
        return
    linked = 0
    seen: set[str] = set()
    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        if native in seen:
            continue
        seen.add(native)
        fa_id = registry.resolve("dmd", native)
        if not fa_id:
            continue
        # Only enrich if the entity already exists (seeded from another
        # source like ChEBI).  DMD-only seeds skip to Pass 2/3.
        if fa_id not in store._entities.index:
            continue
        _add_dmd_to_entity(store, fa_id, native)
        if row["name"]:
            lut.add("chemical", row["name"], fa_id)
        linked += 1

    store._curr_eid = registry.next_eid
    logger.info("Pass 1: DMD enriched %d existing entities.", linked)


# ---------------------------------------------------------------------------
# Pass 2 — Link DMD molecules to existing entities via xrefs
# ---------------------------------------------------------------------------


def link_dmd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    registry: EntityRegistry,
) -> None:
    """Pass 2: link DMD molecules to existing entities via xrefs.

    Tier 1 — ChEBI: 1:1, highest confidence.
    Tier 2 — PubChem: good confidence, may have ambiguous matches.

    Only the DMD native ID is added to linked entities; other xrefs are
    NOT propagated to avoid contaminating curated external_ids.
    """
    molecules = _get_molecules(sources)
    if molecules is None:
        return
    xref_map = _get_xrefs(sources)

    chebi_index = _build_ext_index(store, "chebi")
    pubchem_index = _build_ext_index(store, "pubchem_compound")

    linked_chebi = 0
    linked_pubchem = 0
    ambiguous = 0

    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        # Skip if already resolved to an entity that exists in the store
        # (handled in Pass 1).  Seeded molecules whose entity was NOT
        # created by another source still need xref linking here.
        existing_id = registry.resolve("dmd", native)
        if existing_id and existing_id in store._entities.index:
            continue
        mol_xrefs = xref_map.get(native, {})

        # Tier 1: ChEBI match
        chebi_ids = list(dict.fromkeys(mol_xrefs.get("chebi", [])))
        matched: set[str] = set()
        for cid in chebi_ids:
            fa_id = chebi_index.get(cid)
            if fa_id:
                matched.add(fa_id)

        if matched:
            for fa_id in matched:
                _add_dmd_to_entity(store, fa_id, native)
                registry.register_alias("dmd", native, fa_id)
            linked_chebi += 1 if len(matched) == 1 else 0
            ambiguous += 1 if len(matched) > 1 else 0
            continue

        # Tier 2: PubChem match
        pubchem_ids = mol_xrefs.get("pubchem_cid", [])
        matched = set()
        for pid in pubchem_ids:
            fa_id = pubchem_index.get(pid)
            if fa_id:
                matched.add(fa_id)

        if matched:
            for fa_id in matched:
                _add_dmd_to_entity(store, fa_id, native)
                registry.register_alias("dmd", native, fa_id)
            linked_pubchem += 1 if len(matched) == 1 else 0
            ambiguous += 1 if len(matched) > 1 else 0
            continue

    logger.info(
        "Pass 2: DMD linked %d via ChEBI, %d via PubChem, %d ambiguous.",
        linked_chebi,
        linked_pubchem,
        ambiguous,
    )


# ---------------------------------------------------------------------------
# Pass 3 — Create entities for unlinked DMD molecules
# ---------------------------------------------------------------------------


def create_unlinked_dmd(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    lut: EntityLUT,
    registry: EntityRegistry,
) -> None:
    """Pass 3: create entities for DMD molecules not linked in Pass 1/2.

    Names are disambiguated when multiple DMD molecules share the same
    name by appending the primary xref ID (e.g. UNIPROT:P02662).
    All xrefs are added to the new entity's external_ids.
    """
    molecules = _get_molecules(sources)
    if molecules is None:
        return
    xref_map = _get_xrefs(sources)
    unlinked = _collect_unlinked(molecules, registry, store)

    name_groups: dict[str, list[tuple[str, pd.Series]]] = {}
    for native, row in unlinked:
        name_groups.setdefault(row["name"], []).append((native, row))

    existing_names: set[str] = set()
    if not store._entities.empty:
        existing_names = set(store._entities["common_name"].str.lower().unique())

    created_rows: list[dict] = []
    for name, group in name_groups.items():
        needs_disambig = len(group) > 1 or name.lower() in existing_names
        for native, row in group:
            mol_xrefs = xref_map.get(native, {})

            ext_ids: dict[str, list] = {"dmd": [native]}
            for source, ids in mol_xrefs.items():
                ext_ids[source] = list(dict.fromkeys(ids))

            if needs_disambig:
                suffix = _pick_display_xref(mol_xrefs, native)
                display_name = f"{name} ({suffix})"
            else:
                display_name = name

            existing_id = registry.resolve("dmd", native)
            if existing_id:
                fa_id = existing_id
            else:
                fa_id = f"e{registry.next_eid}"
                registry.register("dmd", native, fa_id)

            data = _build_entity(row, ext_ids, display_name)
            data["foodatlas_id"] = fa_id
            created_rows.append(data)
            if display_name:
                lut.add("chemical", display_name, fa_id)

    store._curr_eid = registry.next_eid
    _append_entities(store, created_rows)
    logger.info("Pass 3: %d unlinked DMD entities.", len(created_rows))
