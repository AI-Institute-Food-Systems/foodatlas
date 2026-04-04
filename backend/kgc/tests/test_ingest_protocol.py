"""Tests for ingest protocol and manifest."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
from src.models.ingest import SourceManifest
from src.pipeline.ingest.protocol import (
    EDGES_COLUMNS,
    NODES_COLUMNS,
    XREFS_COLUMNS,
    ProgressCallback,
    SourceAdapter,
    _noop_progress,
    deserialize_raw_attrs,
    serialize_raw_attrs,
    write_manifest,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_source_manifest_defaults() -> None:
    m = SourceManifest(source_id="test")
    assert m.source_id == "test"
    assert m.node_count == 0
    assert m.edge_count == 0
    assert m.xref_count == 0
    assert m.output_files == []
    assert m.ingest_timestamp  # should be set


def test_source_manifest_with_values() -> None:
    m = SourceManifest(
        source_id="chebi",
        node_count=100,
        edge_count=50,
        xref_count=25,
        raw_dir="/data",
        output_files=["a.parquet", "b.parquet"],
    )
    assert m.node_count == 100
    assert m.raw_dir == "/data"


def test_write_manifest(tmp_path: Path) -> None:
    m = SourceManifest(source_id="test", node_count=42)
    write_manifest(m, tmp_path)
    path = tmp_path / "test_manifest.json"
    assert path.exists()
    data = json.loads(path.read_text())
    assert data["source_id"] == "test"
    assert data["node_count"] == 42


def test_column_constants() -> None:
    assert "source_id" in NODES_COLUMNS
    assert "native_id" in NODES_COLUMNS
    assert "synonyms" in NODES_COLUMNS
    assert "head_native_id" in EDGES_COLUMNS
    assert "edge_type" in EDGES_COLUMNS
    assert "target_source" in XREFS_COLUMNS


def test_deserialize_raw_attrs() -> None:
    df = pd.DataFrame({"raw_attrs": ['{"key": "val"}'], "other": [1]})
    result = deserialize_raw_attrs(df)
    assert result["raw_attrs"].iloc[0] == {"key": "val"}
    assert result["other"].iloc[0] == 1


def test_noop_progress_callable() -> None:
    _noop_progress(0, 100)
    _noop_progress(50, 100)


def test_progress_callback_type() -> None:
    calls: list[tuple[int, int]] = []

    def _tracker(current: int, total: int) -> None:
        calls.append((current, total))

    cb: ProgressCallback = _tracker
    cb(0, 10)
    cb(10, 10)
    assert calls == [(0, 10), (10, 10)]


def test_serialize_and_deserialize_round_trip() -> None:
    df = pd.DataFrame({"raw_attrs": [{"a": 1}, {"b": 2}], "x": [1, 2]})
    serialized = serialize_raw_attrs(df)
    assert serialized["raw_attrs"].iloc[0] == '{"a": 1}'
    restored = deserialize_raw_attrs(serialized)
    assert restored["raw_attrs"].iloc[0] == {"a": 1}


def test_source_adapter_protocol() -> None:
    """Verify a minimal adapter satisfies the protocol."""

    class DummyAdapter:
        @property
        def source_id(self) -> str:
            return "dummy"

        def ingest(self, raw_dir: Path, output_dir: Path) -> SourceManifest:
            return SourceManifest(source_id="dummy")

    adapter = DummyAdapter()
    assert isinstance(adapter, SourceAdapter)
    assert adapter.source_id == "dummy"
