"""Compute and report diffs between registry snapshots across builds."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd

from ..utils.json_io import write_json
from .schema import FILE_BUILD_DIFF, RETIRED_COLUMNS

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


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


def write_diff_report(diff: RegistryDiff, output_dir: Path) -> None:
    """Write ``_build_diff.json`` summarizing what changed."""
    max_samples = 50
    report: dict[str, Any] = {
        "new_count": len(diff.new_ids),
        "retired_count": len(diff.retired_ids),
        "merged_count": len(diff.merged),
        "stable_count": len(diff.stable_ids),
        "new_sample": diff.new_ids[:max_samples],
        "retired_sample": diff.retired_ids[:max_samples],
        "merged_sample": [
            {"old": old, "new": new} for old, new in diff.merged[:max_samples]
        ],
    }
    out = output_dir / FILE_BUILD_DIFF
    write_json(out, report)
    logger.info(
        "Build diff: %d new, %d retired, %d merged, %d stable.",
        report["new_count"],
        report["retired_count"],
        report["merged_count"],
        report["stable_count"],
    )
