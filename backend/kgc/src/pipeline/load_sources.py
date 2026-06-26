"""Load Phase 1 ingest parquet artifacts into memory."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .ingest.protocol import deserialize_raw_attrs

if TYPE_CHECKING:
    from ..models.settings import KGCSettings

logger = logging.getLogger(__name__)

_SOURCE_IDS = [
    "foodon",
    "chebi",
    "cdno",
    "ctd",
    "mesh",
    "pubchem",
    "flavordb",
    "fdc",
    "dmd",
]


def load_sources(settings: KGCSettings) -> dict[str, dict[str, pd.DataFrame]]:
    """Load Phase 1 parquet artifacts into memory.

    Returns:
        ``{source_id: {"nodes": df, "edges": df, "xrefs": df}}``.
    """
    ingest_dir = Path(settings.ingest_dir)
    result: dict[str, dict[str, pd.DataFrame]] = {}

    for source_id in _SOURCE_IDS:
        source_dir = ingest_dir / source_id
        if not source_dir.exists():
            logger.warning("No ingest output for %s.", source_id)
            continue

        data: dict[str, pd.DataFrame] = {}
        for suffix in ("nodes", "edges", "xrefs"):
            path = source_dir / f"{source_id}_{suffix}.parquet"
            if path.exists():
                df = pd.read_parquet(path)
                data[suffix] = deserialize_raw_attrs(df)

        if data:
            result[source_id] = data

    logger.info("Loaded ingest output for %d sources.", len(result))
    return result
