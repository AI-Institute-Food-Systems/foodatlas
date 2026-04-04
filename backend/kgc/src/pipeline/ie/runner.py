"""IERunner — expand KG with information-extraction metadata."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ..knowledge_graph import KnowledgeGraph
from ..triplets.ambiguity import (
    append_ambiguity_jsonl,
    build_ambiguity_from_extractions,
    write_ambiguity_summary,
)
from .loader import load_ie_raw
from .report import write_resolution_stats, write_unresolved_report
from .resolver import resolve_ie_metadata

if TYPE_CHECKING:
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


class IERunner:
    """Orchestrate the IE stage: load, resolve, expand, save."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(self) -> None:
        """Load IE metadata, resolve entities, expand KG, and save."""
        kg = KnowledgeGraph(self._settings)
        self._expand(kg)
        kg.save()

        kg_dir = Path(self._settings.kg_dir)
        report = build_ambiguity_from_extractions(kg.extractions)
        append_ambiguity_jsonl(report, kg_dir)
        write_ambiguity_summary(kg_dir)
        logger.info("IE stage complete.")

    def _expand(self, kg: KnowledgeGraph) -> None:
        """Integrate IE-extracted metadata with lookup-only resolution."""
        ie_config = self._settings.pipeline.stages.triplet_expansion

        if not ie_config.ie_raw_paths:
            logger.info("No IE raw paths configured — skipping expansion.")
            return

        for raw_path in ie_config.ie_raw_paths:
            path = Path(raw_path)
            if not path.exists():
                logger.warning("IE raw file not found at %s — skipping.", path)
                continue

            logger.info("Processing IE file: %s", path)
            metadata = load_ie_raw(path)
            if metadata.empty:
                logger.info("IE loader produced no rows — skipping.")
                continue

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
