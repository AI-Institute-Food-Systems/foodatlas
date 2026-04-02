"""Source adapter protocol and schema constants for Phase 1 ingest."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

    from ...models.ingest import SourceManifest

ProgressCallback = Callable[[int, int], None]
"""Signature: ``callback(current, total)`` — reports row-level progress.

Adapters should call this frequently (e.g., every iteration). Throttling
is handled by the callback implementation, not the caller.
"""


def _noop_progress(_current: int, _total: int) -> None:
    pass


# -- Standard column names for ingest parquet files -----------------------

NODES_COLUMNS = [
    "source_id",
    "native_id",
    "name",
    "synonyms",
    "synonym_types",
    "node_type",
    "raw_attrs",
]

EDGES_COLUMNS = [
    "source_id",
    "head_native_id",
    "tail_native_id",
    "edge_type",
    "raw_attrs",
]

XREFS_COLUMNS = [
    "source_id",
    "native_id",
    "target_source",
    "target_id",
]


@runtime_checkable
class SourceAdapter(Protocol):
    """Protocol that every Phase 1 adapter must satisfy."""

    @property
    def source_id(self) -> str: ...

    def ingest(
        self,
        raw_dir: Path,
        output_dir: Path,
        progress: ProgressCallback = ...,
    ) -> SourceManifest:
        """Read raw files from *raw_dir*, write parquet to *output_dir*.

        Returns a manifest describing what was produced.
        """
        ...


def serialize_raw_attrs(df: pd.DataFrame) -> pd.DataFrame:
    """Convert ``raw_attrs`` column from dicts to JSON strings for parquet."""
    if "raw_attrs" in df.columns:
        df = df.copy()
        df["raw_attrs"] = df["raw_attrs"].apply(json.dumps)
    return df


def deserialize_raw_attrs(df: pd.DataFrame) -> pd.DataFrame:
    """Convert ``raw_attrs`` column from JSON strings back to dicts."""
    if "raw_attrs" in df.columns:
        df = df.copy()
        df["raw_attrs"] = df["raw_attrs"].apply(json.loads)
    return df


def write_manifest(manifest: SourceManifest, output_dir: Path) -> None:
    """Persist a SourceManifest as JSON."""
    data = {
        "source_id": manifest.source_id,
        "ingest_timestamp": manifest.ingest_timestamp,
        "raw_file_hashes": manifest.raw_file_hashes,
        "node_count": manifest.node_count,
        "edge_count": manifest.edge_count,
        "xref_count": manifest.xref_count,
        "raw_dir": manifest.raw_dir,
        "output_files": manifest.output_files,
    }
    path = output_dir / f"{manifest.source_id}_manifest.json"
    path.write_text(json.dumps(data, indent=2))
