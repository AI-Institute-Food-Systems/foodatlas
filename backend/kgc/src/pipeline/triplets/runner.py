"""TripletRunner — build all triplets from Phase 1 edges and IE metadata."""

from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ...utils.json_io import read_json
from ..ingest_loader import load_ingest_output
from ..scaffold import create_empty_triplet_files
from .builder import build_triplets
from .ie_loader import load_ie_raw
from .ie_report import write_resolution_stats, write_unresolved_report
from .ie_resolver import resolve_ie_metadata
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
        self._expand_from_ie(kg)

        kg.save()
        self._validate(kg)
        logger.info("Triplet stage complete.")

    def _expand_from_ie(self, kg: KnowledgeGraph) -> None:
        """Integrate IE-extracted metadata with lookup-only resolution."""
        ie_config = self._settings.pipeline.stages.triplet_expansion

        # Legacy fallback: _metadata_new.json (deprecated).
        old_path = Path(self._settings.kg_dir) / "_metadata_new.json"
        if not ie_config.ie_raw_path and old_path.exists():
            warnings.warn(
                f"Found legacy _metadata_new.json at {old_path}. "
                "Set triplet_expansion.ie_raw_path instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            metadata = pd.DataFrame(read_json(old_path))
            kg.add_triplets_from_metadata(metadata)
            return

        if not ie_config.ie_raw_path:
            logger.info("No IE raw path configured — skipping expansion.")
            return

        path = Path(ie_config.ie_raw_path)
        if not path.is_absolute():
            path = Path(self._settings.data_dir) / ie_config.ie_raw_path
        if not path.exists():
            logger.warning("IE raw file not found at %s — skipping.", path)
            return

        metadata = load_ie_raw(path, ie_config.ie_prob_threshold)
        if metadata.empty:
            logger.info("IE loader produced no rows — skipping.")
            return

        result = resolve_ie_metadata(metadata, kg.entities)

        if not result.resolved.empty:
            n_triplets = kg.add_triplets_from_resolved_ie(result.resolved)
            result.stats["triplets_created"] = n_triplets

        kg_dir = Path(self._settings.kg_dir)
        write_unresolved_report(
            result.unresolved_food,
            result.unresolved_chemical,
            metadata,
            kg_dir,
        )
        write_resolution_stats(result.stats, kg_dir)

    def _validate(self, kg: KnowledgeGraph) -> None:
        """Log basic validation stats."""
        n_ent = len(kg.entities._entities)
        n_tri = len(kg.triplets._triplets)
        logger.info("Validation — entities: %d, triplets: %d", n_ent, n_tri)
        if n_ent == 0:
            logger.warning("KG has zero entities.")
        if n_tri == 0:
            logger.warning("KG has zero triplets.")
