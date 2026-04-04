"""TripletRunner — build all triplets from ingest edges."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ..checkpoint import load_checkpoint, save_checkpoint
from ..knowledge_graph import KnowledgeGraph
from ..load_sources import load_sources
from ..scaffold import create_empty_triplet_files
from .ambiguity import write_ambiguous_attestations
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
        kg_dir = Path(self._settings.kg_dir)
        load_checkpoint(kg_dir, "entities")

        sources = load_sources(self._settings)
        create_empty_triplet_files(self._settings)
        kg = KnowledgeGraph(self._settings)

        build_triplets(kg, sources)

        kg.save()
        self._validate(kg)
        write_ambiguous_attestations(kg.attestations, kg_dir)
        save_checkpoint(kg_dir, "triplets")
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
