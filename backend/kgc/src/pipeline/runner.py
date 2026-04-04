"""Pipeline runner — orchestrates KGC stages in order."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ..utils.timing import log_duration
from .checkpoint import load_checkpoint
from .entities.runner import EntityRunner
from .ie.runner import IERunner
from .ingest.runner import IngestRunner
from .knowledge_graph import KnowledgeGraph
from .load_sources import load_sources
from .postprocessing.flavor import apply_flavor_descriptions
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
        with log_duration("Pipeline", logger):
            for stage in to_run:
                self.run_stage(stage)

    def run_stage(self, stage: PipelineStage) -> None:
        """Run a single stage, logging start/end/duration."""
        with log_duration(f"Stage {stage.name}", logger):
            _STAGE_HANDLERS[stage](self)

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
        kg_dir = Path(self._settings.kg_dir)
        load_checkpoint(kg_dir, "ie")

        sources = load_sources(self._settings)
        kg = KnowledgeGraph(self._settings)
        apply_flavor_descriptions(kg, sources)
        kg.save()


_STAGE_HANDLERS: dict[PipelineStage, Callable[[PipelineRunner], None]] = {
    PipelineStage.INGEST: PipelineRunner._run_ingest,
    PipelineStage.ENTITIES: PipelineRunner._run_entities,
    PipelineStage.TRIPLETS: PipelineRunner._run_triplets,
    PipelineStage.IE: PipelineRunner._run_ie,
    PipelineStage.POSTPROCESSING: PipelineRunner._run_postprocessing,
}
