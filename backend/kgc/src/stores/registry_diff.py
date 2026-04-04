"""Compute and report diffs between registry snapshots across builds."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from .schema import RETIRED_COLUMNS


@dataclass
class RegistryDiff:
    """Difference between old and new entity registry snapshots."""

    new_ids: list[str] = field(default_factory=list)
    retired_ids: list[str] = field(default_factory=list)
    merged: list[tuple[str, str]] = field(default_factory=list)
    stable_ids: list[str] = field(default_factory=list)


def compute_diff(
    old_ids: set[str],
    new_ids: set[str],
    merges: dict[str, str],
) -> RegistryDiff:
    """Compute the diff between old and new entity ID sets.

    Args:
        old_ids: Entity IDs present in the previous registry.
        new_ids: Entity IDs present after the current build.
        merges: Map of old_id → new_id for detected merges.
    """
    merged_sources = set(merges.keys())
    return RegistryDiff(
        new_ids=sorted(new_ids - old_ids),
        retired_ids=sorted(old_ids - new_ids - merged_sources),
        merged=sorted(merges.items()),
        stable_ids=sorted(old_ids & new_ids),
    )


def build_retired_df(diff: RegistryDiff) -> pd.DataFrame:
    """Build a DataFrame for ``retired.parquet`` from the diff."""
    rows: list[dict[str, str]] = []
    for fid in diff.retired_ids:
        rows.append({"foodatlas_id": fid, "action": "retired", "destination": ""})
    for old_id, new_id in diff.merged:
        rows.append({"foodatlas_id": old_id, "action": "merged", "destination": new_id})
    return pd.DataFrame(rows, columns=RETIRED_COLUMNS)
