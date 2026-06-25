"""Tests for load_sources — covers missing source warnings, partial
artifact discovery, and skipping sources that have no matching files."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pandas as pd
from src.pipeline.load_sources import load_sources

if TYPE_CHECKING:
    from pathlib import Path


def _write_source(ingest_dir: Path, source_id: str, kinds: list[str]) -> None:
    """Create a source dir with the given parquet kinds (nodes/edges/xrefs)."""
    source_dir = ingest_dir / source_id
    source_dir.mkdir(parents=True, exist_ok=True)
    for kind in kinds:
        df = pd.DataFrame({"col": [f"{source_id}-{kind}"]})
        df.to_parquet(source_dir / f"{source_id}_{kind}.parquet", index=False)


def test_load_all_three_artifacts(tmp_path: Path) -> None:
    settings = SimpleNamespace(ingest_dir=tmp_path)
    _write_source(tmp_path, "foodon", ["nodes", "edges", "xrefs"])
    result = load_sources(settings)
    assert "foodon" in result
    assert set(result["foodon"].keys()) == {"nodes", "edges", "xrefs"}


def test_partial_artifacts_loaded(tmp_path: Path) -> None:
    """A source with only nodes (no edges/xrefs) still loads what's present."""
    settings = SimpleNamespace(ingest_dir=tmp_path)
    _write_source(tmp_path, "mesh", ["nodes"])
    result = load_sources(settings)
    assert "mesh" in result
    assert list(result["mesh"].keys()) == ["nodes"]


def test_missing_source_dir_logged_and_skipped(tmp_path: Path, caplog: object) -> None:
    """No directory at all for a source => logged warning, not in result."""
    settings = SimpleNamespace(ingest_dir=tmp_path)
    # Only write one of the ten known sources
    _write_source(tmp_path, "chebi", ["nodes"])
    with caplog.at_level(logging.WARNING):  # type: ignore[attr-defined]
        result = load_sources(settings)
    assert "chebi" in result
    # All others were absent
    other_sources = [
        "foodon",
        "cdno",
        "ctd",
        "mesh",
        "pubchem",
        "flavordb",
        "fdc",
        "dmd",
        "bioactivity",
    ]
    for s in other_sources:
        assert s not in result


def test_empty_source_dir_skipped(tmp_path: Path) -> None:
    """A source dir with no parquets at all is not added to the result."""
    settings = SimpleNamespace(ingest_dir=tmp_path)
    (tmp_path / "dmd").mkdir()
    result = load_sources(settings)
    assert "dmd" not in result


def test_returns_empty_when_ingest_dir_has_nothing(tmp_path: Path) -> None:
    settings = SimpleNamespace(ingest_dir=tmp_path)
    result = load_sources(settings)
    assert result == {}
