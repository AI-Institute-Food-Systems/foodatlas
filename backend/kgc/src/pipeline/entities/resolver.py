"""Stage 3: Multi-pass entity resolution from Phase 1 ingest output."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ...stores.entity_store import EntityStore
from ...stores.registry_diff import compute_diff
from ...stores.schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
)
from ...utils.timing import log_duration
from .resolve_dmd import create_chemicals_from_dmd, create_unlinked_dmd, link_dmd
from .resolve_primary import (
    create_chemicals_from_chebi,
    create_diseases_from_ctd,
    create_foods_from_foodon,
)
from .resolve_secondary import (
    create_unlinked_cdno,
    create_unlinked_fdc_foods,
    create_unlinked_fdc_nutrients,
    link_cdno_to_chebi,
    link_fdc_foods_to_foodon,
    link_fdc_nutrients,
)
from .utils.link_xrefs import link_mesh_to_chebi, link_pubchem_to_chebi
from .utils.lut import EntityLUT

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

    from ...config.corrections import Corrections
    from ...stores.entity_registry import EntityRegistry

logger = logging.getLogger(__name__)


class EntityResolver:
    """Three-pass entity resolution from Phase 1 filtered sources.

    Pass 1: Create primary entities from authoritative sources.
    Pass 2: Link secondary sources via cross-references.
    Pass 3: Create entities for unlinked secondary records.
    """

    def __init__(
        self, kg_dir: Path, corrections: Corrections, registry: EntityRegistry
    ) -> None:
        self._kg_dir = kg_dir
        self._corrections = corrections
        self._registry = registry
        self._old_ids: set[str] = registry.all_ids()
        self._entity_store = EntityStore(
            path_entities=kg_dir / FILE_ENTITIES,
            path_lut_food=kg_dir / FILE_LUT_FOOD,
            path_lut_chemical=kg_dir / FILE_LUT_CHEMICAL,
        )
        self._lut = EntityLUT()
        self._linked_native_ids: set[str] = set()

    @property
    def entity_store(self) -> EntityStore:
        return self._entity_store

    @property
    def lut(self) -> EntityLUT:
        return self._lut

    def resolve(self, sources: dict[str, dict[str, pd.DataFrame]]) -> EntityStore:
        """Run all three passes and return the populated EntityStore."""
        with log_duration("Pass 1: primary entities", logger):
            self._pass1_primary(sources)
        with log_duration("Pass 2: link secondary sources", logger):
            self._pass2_link(sources)
        with log_duration("Pass 3: unlinked entities", logger):
            self._pass3_unlinked(sources)
        with log_duration("Rebuild store LUTs", logger):
            self._rebuild_store_luts()
        with log_duration("Finalize registry", logger):
            self._finalize_registry()
        return self._entity_store

    def _pass1_primary(self, sources: dict[str, dict[str, pd.DataFrame]]) -> None:
        create_foods_from_foodon(sources, self._entity_store, self._lut, self._registry)
        create_chemicals_from_chebi(
            sources, self._entity_store, self._lut, self._corrections, self._registry
        )
        create_diseases_from_ctd(sources, self._entity_store, self._lut, self._registry)
        create_chemicals_from_dmd(
            sources, self._entity_store, self._lut, self._registry
        )
        logger.info("Pass 1 complete: %d entities.", len(self._entity_store._entities))

    def _pass2_link(self, sources: dict[str, dict[str, pd.DataFrame]]) -> None:
        reg = self._registry
        link_cdno_to_chebi(sources, self._entity_store, reg)
        link_fdc_foods_to_foodon(
            sources,
            self._entity_store,
            self._corrections,
            self._linked_native_ids,
            reg,
        )
        link_fdc_nutrients(sources, self._entity_store, self._linked_native_ids, reg)
        # PubChem and MeSH xrefs enrich external_ids on entities.
        # Now also registered in the registry via the seeder for lookup.
        link_pubchem_to_chebi(sources, self._entity_store)
        link_mesh_to_chebi(sources, self._entity_store)
        link_dmd(sources, self._entity_store, reg)
        logger.info("Pass 2 complete: %d entities.", len(self._entity_store._entities))

    def _pass3_unlinked(self, sources: dict[str, dict[str, pd.DataFrame]]) -> None:
        create_unlinked_cdno(sources, self._entity_store, self._lut, self._registry)
        create_unlinked_fdc_foods(
            sources,
            self._entity_store,
            self._lut,
            self._linked_native_ids,
            self._registry,
        )
        create_unlinked_fdc_nutrients(
            sources,
            self._entity_store,
            self._lut,
            self._linked_native_ids,
            self._registry,
        )
        create_unlinked_dmd(sources, self._entity_store, self._lut, self._registry)
        logger.info("Pass 3 complete: %d entities.", len(self._entity_store._entities))

    def _rebuild_store_luts(self) -> None:
        self._entity_store._lut_food = self._lut.get_food_lut()
        self._entity_store._lut_chemical = self._lut.get_chemical_lut()

    def _finalize_registry(self) -> None:
        """Compute the registry diff for logging, then save the registry."""
        new_ids = self._registry.all_ids()
        diff = compute_diff(self._old_ids, new_ids, {})
        self._registry.save()

        logger.info(
            "Registry diff: %d stable, %d new, %d retired, %d merged.",
            len(diff.stable_ids),
            len(diff.new_ids),
            len(diff.retired_ids),
            len(diff.merged),
        )
