"""The KG snapshot a delivery was built against — the only place the entity
ids it cites can be de-referenced."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .cells import as_dict, as_list

if TYPE_CHECKING:
    from pathlib import Path


def latest_snapshot(previous_kg_dir: Path) -> Path:
    """The newest ``PreviousFAKG/<timestamp>/`` that holds an entities file."""
    snapshots = sorted(
        path
        for path in previous_kg_dir.iterdir()
        if (path / "entities.parquet").is_file()
    )
    if not snapshots:
        msg = f"no KG snapshot with entities.parquet under {previous_kg_dir}"
        raise FileNotFoundError(msg)
    return snapshots[-1]


def load_snapshot_entities(snapshot_dir: Path) -> pd.DataFrame:
    """Snapshot entities indexed by foodatlas_id, JSON columns parsed."""
    entities = pd.read_parquet(snapshot_dir / "entities.parquet")
    entities["external_ids"] = entities["external_ids"].map(as_dict)
    entities["synonyms"] = entities["synonyms"].map(as_list)
    return entities.set_index("foodatlas_id")


def ids_of_type(entities: pd.DataFrame, entity_type: str) -> set[str]:
    return set(entities.index[entities["entity_type"] == entity_type])
