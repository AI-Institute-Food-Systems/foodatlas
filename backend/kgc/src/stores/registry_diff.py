"""Compute diffs between registry snapshots across builds.

Used for structural logging only — we report how many IDs were stable,
added, retired, or merged between registry snapshots, but we do not
currently persist the retired set. If/when a forwarding table becomes
a product requirement, re-introduce a writer here.
"""

from __future__ import annotations

from dataclasses import dataclass, field


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
