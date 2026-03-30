"""Pipeline runner — orchestrates KGC stages in order."""

from __future__ import annotations

import datetime
import hashlib
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ..construct.runner import ConstructRunner
from ..constructor.knowledge_graph import KnowledgeGraph
from ..ingest.runner import IngestRunner
from ..utils.json_io import read_json, write_json
from .stages import ALL_STAGES, PipelineStage

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
        self._kg: KnowledgeGraph | None = None

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

    def _ensure_kg(self) -> KnowledgeGraph:
        """Lazily load the KnowledgeGraph."""
        if self._kg is None:
            self._kg = KnowledgeGraph(self._settings)
        return self._kg

    # ------------------------------------------------------------------
    # Stage handlers
    # ------------------------------------------------------------------

    def _run_ingest(self) -> None:
        runner = IngestRunner(self._settings)
        runner.run(sources=getattr(self, "_sources", None))

    def _run_construct_full(self) -> None:
        """Run the full Phase 2 construct pipeline."""
        runner = ConstructRunner(self._settings)
        runner.run()

    def _run_corrections(self) -> None:
        logger.info("Run as part of construct pipeline (use stages together).")

    def _run_subtree_filter(self) -> None:
        logger.info("Run as part of construct pipeline (use stages together).")

    def _run_entity_resolution(self) -> None:
        logger.info("Run as part of construct pipeline (use stages together).")

    def _run_triplet_build(self) -> None:
        logger.info("Run as part of construct pipeline (use stages together).")

    def _run_metadata_processing(self) -> None:
        logger.info("Metadata processing is handled by the IE pipeline.")

    def _run_triplet_expansion(self) -> None:
        kg_dir = Path(self._settings.kg_dir)
        metadata_path = kg_dir / "_metadata_new.json"
        if not metadata_path.exists():
            logger.warning("No metadata at %s — skipping.", metadata_path)
            return

        kg = self._ensure_kg()
        metadata = pd.DataFrame(read_json(metadata_path))
        kg.add_triplets_from_metadata(metadata)
        kg.save()

        logger.info("Triplet expansion complete — running validation.")
        self._validate_kg()

    def _run_postprocessing(self) -> None:
        logger.info("Run as part of construct pipeline (use stages together).")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_kg(self) -> None:
        """Basic validation after triplet expansion."""
        kg = self._ensure_kg()
        n_ent = len(kg.entities._entities)
        n_tri = len(kg.triplets._triplets)
        logger.info("Validation — entities: %d, triplets: %d", n_ent, n_tri)
        if n_ent == 0:
            logger.warning("KG has zero entities after triplet expansion.")
        if n_tri == 0:
            logger.warning("KG has zero triplets after triplet expansion.")

    def _write_version(self) -> None:
        """Write version.json after a full pipeline run."""
        version_info: dict[str, object] = {
            "timestamp": datetime.datetime.now(tz=datetime.UTC).isoformat(),
            "stages": [s.name for s in ALL_STAGES],
            "version": "0.1.0",
        }

        kg_dir = Path(self._settings.kg_dir)
        entities_path = kg_dir / "entities.json"
        if entities_path.exists():
            h = hashlib.sha256(entities_path.read_bytes()).hexdigest()[:12]
            version_info["entities_hash"] = h

        version_path = kg_dir / "version.json"
        write_json(version_path, version_info)
        logger.info("Wrote %s", version_path)


_STAGE_HANDLERS: dict[PipelineStage, Callable[[PipelineRunner], None]] = {
    PipelineStage.INGEST: PipelineRunner._run_ingest,
    PipelineStage.CORRECTIONS: PipelineRunner._run_construct_full,
    PipelineStage.SUBTREE_FILTER: PipelineRunner._run_subtree_filter,
    PipelineStage.ENTITY_RESOLUTION: PipelineRunner._run_entity_resolution,
    PipelineStage.TRIPLET_BUILD: PipelineRunner._run_triplet_build,
    PipelineStage.METADATA_PROCESSING: PipelineRunner._run_metadata_processing,
    PipelineStage.TRIPLET_EXPANSION: PipelineRunner._run_triplet_expansion,
    PipelineStage.POSTPROCESSING: PipelineRunner._run_postprocessing,
}
