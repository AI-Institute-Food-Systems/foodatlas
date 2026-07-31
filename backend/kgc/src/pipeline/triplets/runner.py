"""TripletRunner — build all triplets from ingest edges."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ...utils.timing import log_duration
from ..checkpoint import load_checkpoint, save_checkpoint
from ..knowledge_graph import KnowledgeGraph
from ..load_sources import load_sources
from ..scaffold import create_empty_triplet_files
from .ambiguity import write_ambiguous_attestations
from .bioactivity import (
    promote_bioactivity_disease,
    promote_bioactivity_measurements,
    promote_bioassays,
    promote_food_chemical_efficacy,
)
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
        with log_duration("Load checkpoint (entities)", logger):
            load_checkpoint(kg_dir, "entities")
        with log_duration("Load ingest sources", logger):
            sources = load_sources(self._settings)
        create_empty_triplet_files(self._settings)
        with log_duration("Load KnowledgeGraph", logger):
            kg = KnowledgeGraph(self._settings)

        with log_duration("Build all triplets", logger):
            build_triplets(kg, sources)

        with log_duration("Save KG", logger):
            kg.save()
        with log_duration("Promote bioactivity measurements", logger):
            promote_bioactivity_measurements(self._settings, kg)
        with log_duration("Promote bioassays", logger):
            promote_bioassays(self._settings)
        with log_duration("Promote food-chemical efficacy", logger):
            promote_food_chemical_efficacy(self._settings)
        with log_duration("Promote bioactivity-disease bridge", logger):
            promote_bioactivity_disease(self._settings)
        self._validate(kg)
        with log_duration("Write ambiguous attestations", logger):
            write_ambiguous_attestations(kg.attestations, kg_dir)
        with log_duration("Save checkpoint (triplets)", logger):
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
