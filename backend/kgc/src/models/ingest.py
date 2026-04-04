"""Data models for the Phase 1 ingest layer."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SourceManifest:
    """Metadata produced by a single source adapter run."""

    source_id: str
    ingest_timestamp: str = field(
        default_factory=lambda: datetime.datetime.now(tz=datetime.UTC).isoformat()
    )
    raw_file_hashes: dict[str, str] = field(default_factory=dict)
    node_count: int = 0
    edge_count: int = 0
    xref_count: int = 0
    raw_dir: str = ""
    output_files: list[str] = field(default_factory=list)
