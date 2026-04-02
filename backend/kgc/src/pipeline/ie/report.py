"""Write IE resolution reports: unresolved names and aggregate stats."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from ...stores.schema import FILE_IE_RESOLUTION_STATS, FILE_IE_UNRESOLVED
from ...utils.json_io import write_json

logger = logging.getLogger(__name__)


def write_unresolved_report(
    unresolved_food: set[str],
    unresolved_chemical: set[str],
    metadata: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """Write ``_ie_unresolved.tsv`` sorted by occurrence count descending."""
    rows: list[dict] = []

    for name in unresolved_food:
        occ = int((metadata["_food_name"] == name).sum())
        refs = (
            metadata.loc[metadata["_food_name"] == name, "reference"].head(3).tolist()
        )
        rows.append(
            {
                "name": name,
                "entity_type": "food",
                "occurrence_count": occ,
                "sample_references": "; ".join(
                    r[0] if isinstance(r, list) and r else str(r) for r in refs
                ),
            }
        )

    for name in unresolved_chemical:
        occ = int((metadata["_chemical_name"] == name).sum())
        refs = (
            metadata.loc[metadata["_chemical_name"] == name, "reference"]
            .head(3)
            .tolist()
        )
        rows.append(
            {
                "name": name,
                "entity_type": "chemical",
                "occurrence_count": occ,
                "sample_references": "; ".join(
                    r[0] if isinstance(r, list) and r else str(r) for r in refs
                ),
            }
        )

    columns = ["name", "entity_type", "occurrence_count", "sample_references"]
    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df = df.sort_values("occurrence_count", ascending=False)

    out = Path(output_dir) / FILE_IE_UNRESOLVED
    df.to_csv(out, sep="\t", index=False)
    logger.info("Wrote %d unresolved names to %s.", len(df), out)
    return out


def write_resolution_stats(
    stats: dict[str, Any],
    output_dir: Path,
) -> Path:
    """Write ``_ie_resolution_stats.json``."""
    out = Path(output_dir) / FILE_IE_RESOLUTION_STATS
    write_json(out, stats)
    logger.info("Wrote IE resolution stats to %s.", out)
    return out
