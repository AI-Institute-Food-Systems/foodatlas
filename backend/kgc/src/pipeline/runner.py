"""Pipeline runner — orchestrates KGC stages in order."""

from __future__ import annotations

import datetime
import hashlib
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from ..utils.json_io import write_json
from .entities.runner import EntityRunner
from .ie.runner import IERunner
from .ingest.runner import IngestRunner
from .stages import ALL_STAGES, PipelineStage
from .triplets.runner import TripletRunner

if TYPE_CHECKING:
    from collections.abc import Callable

    from ..models.settings import KGCSettings

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Run KGC pipeline stages in order.

    Args:
        settings: KGCSettings instance with all paths and credentials.
    """

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(
        self,
        stages: list[PipelineStage] | None = None,
        sources: list[str] | None = None,
    ) -> None:
        """Run all or selected stages in order."""
        self._sources = sources
        to_run = sorted(stages or ALL_STAGES, key=lambda s: s.value)

        logger.info("Pipeline starting — stages: %s", [s.name for s in to_run])
        t0 = time.monotonic()

        for stage in to_run:
            self.run_stage(stage)

        elapsed = time.monotonic() - t0
        logger.info("Pipeline finished in %.1fs", elapsed)

        if stages is None:
            self._write_version()

    def run_stage(self, stage: PipelineStage) -> None:
        """Run a single stage, logging start/end/duration."""
        logger.info("=== Stage %s START ===", stage.name)
        t0 = time.monotonic()

        _STAGE_HANDLERS[stage](self)

        elapsed = time.monotonic() - t0
        logger.info("=== Stage %s END (%.1fs) ===", stage.name, elapsed)

    # ------------------------------------------------------------------
    # Stage handlers
    # ------------------------------------------------------------------

    def _run_ingest(self) -> None:
        runner = IngestRunner(self._settings)
        runner.run(sources=getattr(self, "_sources", None))

    def _run_entities(self) -> None:
        runner = EntityRunner(self._settings)
        runner.run()

    def _run_triplets(self) -> None:
        runner = TripletRunner(self._settings)
        runner.run()

    def _run_ie(self) -> None:
        runner = IERunner(self._settings)
        runner.run()

    def _run_postprocessing(self) -> None:
        logger.info("Postprocessing is deferred — not yet implemented.")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _write_version(self) -> None:
        """Write version.json after a full pipeline run."""
        version_info: dict[str, object] = {
            "timestamp": datetime.datetime.now(tz=datetime.UTC).isoformat(),
            "stages": [s.name for s in ALL_STAGES],
            "version": "0.1.0",
        }

        kg_dir = Path(self._settings.kg_dir)
        entities_path = kg_dir / "entities.parquet"
        if entities_path.exists():
            h = hashlib.sha256(entities_path.read_bytes()).hexdigest()[:12]
            version_info["entities_hash"] = h

        version_path = kg_dir / "version.json"
        write_json(version_path, version_info)
        logger.info("Wrote %s", version_path)


_STAGE_HANDLERS: dict[PipelineStage, Callable[[PipelineRunner], None]] = {
    PipelineStage.INGEST: PipelineRunner._run_ingest,
    PipelineStage.ENTITIES: PipelineRunner._run_entities,
    PipelineStage.TRIPLETS: PipelineRunner._run_triplets,
    PipelineStage.IE: PipelineRunner._run_ie,
    PipelineStage.POSTPROCESSING: PipelineRunner._run_postprocessing,
}
