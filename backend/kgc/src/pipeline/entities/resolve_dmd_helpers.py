"""Shared helpers for DMD entity resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from ...models.entity import ChemicalEntity

if TYPE_CHECKING:
    from ...stores.entity_registry import EntityRegistry
    from ...stores.entity_store import EntityStore

# Preferred xref source order for display-name disambiguation.
_XREF_PREF = ["uniprot", "chebi", "pubchem_cid", "kegg", "hmdb", "mirbase"]


def _get_molecules(
    sources: dict[str, dict[str, pd.DataFrame]],
) -> pd.DataFrame | None:
    dmd = sources.get("dmd")
    if dmd is None:
        return None
    nodes = dmd["nodes"]
    return nodes[nodes["node_type"] == "molecule"]


def _get_xrefs(
    sources: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, dict[str, list[str]]]:
    """Build ``{dmd_native_id: {target_source: [target_ids]}}``."""
    dmd = sources.get("dmd")
    if dmd is None:
        return {}
    xrefs_df = dmd.get("xrefs", pd.DataFrame())
    if xrefs_df.empty:
        return {}
    result: dict[str, dict[str, list[str]]] = {}
    for _, row in xrefs_df.iterrows():
        result.setdefault(row["native_id"], {}).setdefault(
            row["target_source"], []
        ).append(row["target_id"])
    return result


def _pick_display_xref(
    xrefs: dict[str, list[str]],
    native_id: str,
) -> str:
    """Pick the best xref for a display-name suffix."""
    for source in _XREF_PREF:
        ids = xrefs.get(source, [])
        if ids:
            return f"{source.upper()}:{ids[0]}"
    return native_id


def _build_entity(
    row: pd.Series,
    external_ids: dict[str, list],
    display_name: str,
) -> dict[str, object]:
    """Build an entity dict from a DMD molecule row."""
    synonyms_raw = row.get("synonyms", [])
    # Parquet returns numpy arrays, not Python lists.
    if isinstance(synonyms_raw, np.ndarray):
        synonyms_raw = synonyms_raw.tolist()
    if not isinstance(synonyms_raw, list):
        synonyms_raw = []

    syns: list[str] = [display_name] if display_name else []
    # Add original name as synonym if different from display name.
    if row["name"] and row["name"] != display_name and row["name"] not in syns:
        syns.append(row["name"])
    # Add extra synonyms (chemical formula, protein sequence, etc.).
    for s in synonyms_raw:
        if s and s not in syns:
            syns.append(s)

    entity = ChemicalEntity(
        foodatlas_id="",  # caller sets this
        common_name=display_name,
        synonyms=syns,
        external_ids=external_ids,
    )
    result: dict[str, object] = entity.model_dump(by_alias=True)
    return result


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])


def _add_dmd_to_entity(store: EntityStore, fa_id: str, native: str) -> None:
    """Append a DMD native ID to an existing entity's external_ids."""
    ext = store._entities.at[fa_id, "external_ids"]
    if "dmd" not in ext:
        ext["dmd"] = []
    if native not in ext["dmd"]:
        ext["dmd"].append(native)


def _collect_unlinked(
    molecules: pd.DataFrame,
    registry: EntityRegistry,
    store: EntityStore,
) -> list[tuple[str, pd.Series]]:
    """Collect DMD molecules not yet resolved to an entity in the store."""
    unlinked: list[tuple[str, pd.Series]] = []
    seen: set[str] = set()
    for _, row in molecules.iterrows():
        native = str(row["native_id"])
        if native in seen:
            continue
        seen.add(native)
        existing_ids = registry.resolve("dmd", native)
        if any(eid in store._entities.index for eid in existing_ids):
            continue
        unlinked.append((native, row))
    return unlinked
