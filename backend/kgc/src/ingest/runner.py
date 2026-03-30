"""IngestRunner — orchestrates Phase 1 source adapters in parallel."""

from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

from .adapters.cdno import CDNOAdapter
from .adapters.chebi import ChEBIAdapter
from .adapters.ctd import CTDAdapter
from .adapters.fdc import FDCAdapter
from .adapters.flavordb import FlavorDBAdapter
from .adapters.foodon import FoodOnAdapter
from .adapters.mesh import MeSHAdapter
from .adapters.pubchem import PubChemAdapter

if TYPE_CHECKING:
    from ..models.ingest import SourceManifest
    from ..models.settings import KGCSettings
    from .protocol import SourceAdapter

logger = logging.getLogger(__name__)

ALL_ADAPTERS: list[type] = [
    FoodOnAdapter,
    ChEBIAdapter,
    CDNOAdapter,
    CTDAdapter,
    MeSHAdapter,
    PubChemAdapter,
    FlavorDBAdapter,
    FDCAdapter,
]


def _run_single_adapter(
    adapter_cls: type,
    raw_dir: str,
    output_dir: str,
) -> SourceManifest:
    """Run one adapter — used as target for ProcessPoolExecutor."""
    adapter: SourceAdapter = adapter_cls()
    out = Path(output_dir) / adapter.source_id
    return adapter.ingest(Path(raw_dir), out)


class IngestRunner:
    """Run all source adapters, producing standardized parquet."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(
        self,
        sources: list[str] | None = None,
    ) -> dict[str, SourceManifest]:
        """Run adapters in parallel.

        Args:
            sources: Optional list of source IDs to run. None runs all.

        Returns:
            Mapping of source_id to SourceManifest.
        """
        raw_dir = self._settings.data_dir
        output_dir = self._settings.ingest_dir

        adapters_to_run = ALL_ADAPTERS
        if sources:
            adapters_to_run = [
                cls for cls in ALL_ADAPTERS if cls().source_id in sources
            ]

        logger.info("Launching %d ingest adapters.", len(adapters_to_run))
        results: dict[str, SourceManifest] = {}

        with ProcessPoolExecutor(max_workers=len(adapters_to_run)) as pool:
            futures = {
                pool.submit(_run_single_adapter, cls, raw_dir, output_dir): cls
                for cls in adapters_to_run
            }
            for future in as_completed(futures):
                futures[future]
                manifest = future.result()
                results[manifest.source_id] = manifest
                logger.info("Adapter %s finished.", manifest.source_id)

        logger.info("All ingest adapters complete.")
        return results
