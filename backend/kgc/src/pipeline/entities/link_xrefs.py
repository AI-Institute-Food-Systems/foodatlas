"""Pass 2: Link PubChem and MeSH cross-references to ChEBI entities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore

logger = logging.getLogger(__name__)


def _build_chebi_to_fa(store: EntityStore) -> dict[int, str]:
    result: dict[int, str] = {}
    for eid, row in store._entities.iterrows():
        for cid in row["external_ids"].get("chebi", []):
            result[int(cid)] = str(eid)
    return result


def _build_external_index_int(store: EntityStore, key: str) -> dict[int, str]:
    """Build int-keyed external_ids index (e.g. pubchem_compound)."""
    result: dict[int, str] = {}
    for eid, row in store._entities.iterrows():
        for val in row["external_ids"].get(key, []):
            result[int(val)] = str(eid)
    return result


def link_pubchem_to_chebi(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    registry: EntityRegistry,
    merges: dict[str, str],
) -> None:
    """Add pubchem_compound IDs to ChEBI entities via PubChem xrefs."""
    pubchem = sources.get("pubchem")
    if pubchem is None:
        return
    xrefs = pubchem.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return
    chebi_xrefs = xrefs[xrefs["target_source"] == "chebi"]
    chebi2fa = _build_chebi_to_fa(store)

    linked = 0
    for _, xref in chebi_xrefs.iterrows():
        chebi_ref = xref["target_id"]
        chebi_id = chebi_ref.split(":")[-1] if ":" in chebi_ref else chebi_ref
        fa_id = chebi2fa.get(int(chebi_id))
        if fa_id is None:
            continue
        ext = store._entities.at[fa_id, "external_ids"]
        if "pubchem_compound" not in ext:
            ext["pubchem_compound"] = []
        pc_cid = int(xref["native_id"])
        if pc_cid not in ext["pubchem_compound"]:
            ext["pubchem_compound"].append(pc_cid)
            old = registry.register_alias("pubchem_compound", str(pc_cid), fa_id)
            if old:
                merges[old] = fa_id
            linked += 1
    logger.info("Pass 2: linked %d PubChem CIDs to ChEBI.", linked)


def link_mesh_to_chebi(
    sources: dict[str, dict[str, pd.DataFrame]],
    store: EntityStore,
    registry: EntityRegistry,
    merges: dict[str, str],
) -> None:
    """Add MeSH IDs to ChEBI entities via PubChem CID → MeSH name → MeSH ID."""
    pubchem = sources.get("pubchem")
    mesh = sources.get("mesh")
    if pubchem is None or mesh is None:
        return
    xrefs = pubchem.get("xrefs", pd.DataFrame())
    if xrefs.empty:
        return

    mesh_term_xrefs = xrefs[xrefs["target_source"] == "mesh_term"]
    cid_to_mesh_names: dict[int, list[str]] = {}
    for _, row in mesh_term_xrefs.iterrows():
        cid = int(row["native_id"])
        if cid not in cid_to_mesh_names:
            cid_to_mesh_names[cid] = []
        cid_to_mesh_names[cid].append(row["target_id"].lower())

    mesh_nodes = mesh.get("nodes", pd.DataFrame())
    name_to_ids: dict[str, list[str]] = {}
    for _, row in mesh_nodes.iterrows():
        n = row["name"].lower()
        if n not in name_to_ids:
            name_to_ids[n] = []
        name_to_ids[n].append(row["native_id"])

    pc_index = _build_external_index_int(store, "pubchem_compound")

    linked = 0
    for pc_cid, fa_id in pc_index.items():
        for mesh_name in cid_to_mesh_names.get(pc_cid, []):
            for mesh_id in name_to_ids.get(mesh_name, []):
                ext = store._entities.at[fa_id, "external_ids"]
                if "mesh" not in ext:
                    ext["mesh"] = []
                if mesh_id not in ext["mesh"]:
                    ext["mesh"].append(mesh_id)
                    old = registry.register_alias("mesh", str(mesh_id), fa_id)
                    if old:
                        merges[old] = fa_id
                    linked += 1
    logger.info("Pass 2: linked %d MeSH IDs to ChEBI.", linked)
