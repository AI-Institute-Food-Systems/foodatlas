"""Tests for utils.snapshots — locating PreviousFAKG snapshots."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.utils.snapshots import latest_snapshot

if TYPE_CHECKING:
    from pathlib import Path


def test_returns_none_when_subdir_missing(tmp_path: Path) -> None:
    assert latest_snapshot(tmp_path) is None


def test_returns_none_when_subdir_empty(tmp_path: Path) -> None:
    (tmp_path / "PreviousFAKG").mkdir()
    assert latest_snapshot(tmp_path) is None


def test_picks_newest_by_name(tmp_path: Path) -> None:
    base = tmp_path / "PreviousFAKG"
    base.mkdir()
    (base / "20260101T000000Z").mkdir()
    (base / "20260601T000000Z").mkdir()
    assert latest_snapshot(tmp_path).name == "20260601T000000Z"


def test_skips_symlinks(tmp_path: Path) -> None:
    base = tmp_path / "PreviousFAKG"
    base.mkdir()
    real = base / "20260101T000000Z"
    real.mkdir()
    (base / "zzz_latest").symlink_to(real)  # sorts last but must be ignored
    assert latest_snapshot(tmp_path).name == "20260101T000000Z"
