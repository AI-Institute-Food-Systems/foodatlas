"""EntityRunner — filter sources and resolve entities from Phase 1 output."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ...config.corrections import load_corrections
from ...stores.entity_registry import EntityRegistry
from ...stores.schema import FILE_REGISTRY
from ..load_sources import load_sources
from ..scaffold import create_empty_entity_files, ensure_registry_exists
from .resolver import EntityResolver
from .utils.subtree_filter import filter_sources

if TYPE_CHECKING:
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


class EntityRunner:
    """Orchestrate the ENTITIES stage: filter → resolve → save."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(self) -> None:
        """Load ingest output, filter, resolve entities, and save."""
        sources = load_sources(self._settings)
        corrections = load_corrections()

        logger.info("=== SUBTREE FILTER ===")
        filter_sources(sources, corrections.ontology_roots)

        logger.info("=== ENTITY RESOLUTION ===")
        kg_dir = Path(self._settings.kg_dir)
        ensure_registry_exists(self._settings)
        registry = EntityRegistry(kg_dir / FILE_REGISTRY)

        create_empty_entity_files(self._settings)
        resolver = EntityResolver(kg_dir, corrections, registry)
        resolver.resolve(sources)
        resolver.entity_store.save(kg_dir)

        logger.info("Entity resolution complete.")
