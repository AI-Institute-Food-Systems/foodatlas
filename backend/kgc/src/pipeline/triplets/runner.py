"""TripletRunner — build all triplets from ingest edges."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ...stores.schema import FILE_AMBIGUITY_JSONL
from ..knowledge_graph import KnowledgeGraph
from ..load_sources import load_sources
from ..scaffold import create_empty_triplet_files
from .ambiguity import append_ambiguity_jsonl
from .builder import build_triplets

if TYPE_CHECKING:
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


class TripletRunner:
    """Orchestrate the TRIPLETS stage: build + save."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(self) -> None:
        """Build triplets from ingest edges and save."""
        sources = load_sources(self._settings)

        create_empty_triplet_files(self._settings)
        kg = KnowledgeGraph(self._settings)

        # Clear ambiguity JSONL for fresh pipeline run.
        kg_dir = Path(self._settings.kg_dir)
        jsonl = kg_dir / FILE_AMBIGUITY_JSONL
        if jsonl.exists():
            jsonl.unlink()

        report = build_triplets(kg, sources)

        kg.save()
        self._validate(kg)
        append_ambiguity_jsonl(report, kg_dir)
        logger.info("Triplet stage complete.")

    def _validate(self, kg: KnowledgeGraph) -> None:
        """Log basic validation stats."""
        n_ent = len(kg.entities._entities)
        n_tri = len(kg.triplets._triplets)
        logger.info("Validation — entities: %d, triplets: %d", n_ent, n_tri)
        if n_ent == 0:
            logger.warning("KG has zero entities.")
        if n_tri == 0:
            logger.warning("KG has zero triplets.")
