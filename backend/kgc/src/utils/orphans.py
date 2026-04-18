"""Orphan entity detection: entities not referenced by any triplet."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd


def find_orphans(ents: pd.DataFrame, trips: pd.DataFrame) -> pd.DataFrame:
    """Return entities not referenced by any triplet head or tail."""
    referenced = set(trips["head_id"]).union(trips["tail_id"])
    return ents[~ents.index.isin(referenced)]


def orphan_counts_by_type(
    ents: pd.DataFrame,
    trips: pd.DataFrame,
) -> dict[str, int]:
    """Count orphan entities grouped by entity_type."""
    counts: dict[str, int] = (
        find_orphans(ents, trips)["entity_type"].value_counts().to_dict()
    )
    return counts


def write_orphans_jsonl(
    ents: pd.DataFrame,
    trips: pd.DataFrame,
    out_path: Path,
) -> int:
    """Write orphan entities to *out_path* as JSONL. Returns count written."""
    orphans = find_orphans(ents, trips)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for eid, row in orphans.iterrows():
            record = {
                "foodatlas_id": eid,
                "entity_type": row.get("entity_type", ""),
                "common_name": row.get("common_name", ""),
            }
            f.write(json.dumps(record) + "\n")
    return len(orphans)
