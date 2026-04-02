"""TripletRunner — build all triplets from Phase 1 edges and IE metadata."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ...utils.json_io import read_json
from ..ingest_loader import load_ingest_output
from ..scaffold import create_empty_triplet_files
from .builder import build_triplets
from .knowledge_graph import KnowledgeGraph

if TYPE_CHECKING:
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


class TripletRunner:
    """Orchestrate the TRIPLETS stage: build + expand + save."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(self) -> None:
        """Build triplets from Phase 1 edges, expand from IE metadata, save."""
        sources = load_ingest_output(self._settings)

        create_empty_triplet_files(self._settings)
        kg = KnowledgeGraph(self._settings)

        build_triplets(kg, sources)
        self._expand_from_metadata(kg)

        kg.save()
        self._validate(kg)
        logger.info("Triplet stage complete.")

    def _expand_from_metadata(self, kg: KnowledgeGraph) -> None:
        """Integrate IE-extracted metadata into the KG, if available."""
        metadata_path = Path(self._settings.kg_dir) / "_metadata_new.json"
        if not metadata_path.exists():
            logger.info("No IE metadata at %s — skipping expansion.", metadata_path)
            return

        metadata = pd.DataFrame(read_json(metadata_path))
        kg.add_triplets_from_metadata(metadata)
        logger.info("Triplet expansion from IE metadata complete.")

    def _validate(self, kg: KnowledgeGraph) -> None:
        """Log basic validation stats."""
        n_ent = len(kg.entities._entities)
        n_tri = len(kg.triplets._triplets)
        logger.info("Validation — entities: %d, triplets: %d", n_ent, n_tri)
        if n_ent == 0:
            logger.warning("KG has zero entities.")
        if n_tri == 0:
            logger.warning("KG has zero triplets.")
