"""Locate previous-KG snapshots under ``data/PreviousFAKG/``."""

from __future__ import annotations

from pathlib import Path

SNAPSHOT_SUBDIR = "PreviousFAKG"


def latest_snapshot(data_dir: str | Path) -> Path | None:
    """Return the most recent snapshot directory, or None if there are none.

    Snapshot directories are named by UTC timestamp, so lexicographic sort
    matches chronological order. Symlinks are skipped (e.g. a ``latest`` alias).
    """
    base = Path(data_dir) / SNAPSHOT_SUBDIR
    if not base.is_dir():
        return None
    snapshots = sorted(
        (d for d in base.iterdir() if d.is_dir() and not d.is_symlink()),
        key=lambda d: d.name,
    )
    return snapshots[-1] if snapshots else None
