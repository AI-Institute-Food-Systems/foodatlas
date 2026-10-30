"""Small EntityStore mutations shared by the entity-resolution passes."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...stores.entity_store import EntityStore


def _append_entities(store: EntityStore, rows: list[dict]) -> None:
    if rows:
        new_df = pd.DataFrame(rows).set_index("foodatlas_id")
        store._entities = pd.concat([store._entities, new_df])


def _extract_chebi_int(chebi_ref: str) -> int:
    if "CHEBI_" in chebi_ref:
        return int(chebi_ref.rsplit("CHEBI_", maxsplit=1)[-1])
    return int(chebi_ref)


def _add_ext_id(store: EntityStore, fa_id: str, key: str, value: object) -> None:
    """Append *value* to entity's external_ids[key] if not present."""
    ext = store._entities.at[fa_id, "external_ids"]
    if key not in ext:
        ext[key] = []
    if value not in ext[key]:
        ext[key].append(value)
