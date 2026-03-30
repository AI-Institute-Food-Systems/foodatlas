"""Stage 3: Multi-pass entity resolution from Phase 1 ingest output."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..stores.entity_store import EntityStore
from ..stores.schema import FILE_ENTITIES, FILE_LUT_CHEMICAL, FILE_LUT_FOOD
from .entity_lut import EntityLUT
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

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

    from ..config.corrections import Corrections

logger = logging.getLogger(__name__)


class EntityResolver:
    """Three-pass entity resolution from Phase 1 filtered sources.

    Pass 1: Create primary entities from authoritative sources.
    Pass 2: Link secondary sources via cross-references.
    Pass 3: Create entities for unlinked secondary records.
    """

    def __init__(self, kg_dir: Path, corrections: Corrections) -> None:
        self._kg_dir = kg_dir
        self._corrections = corrections
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
        self._pass1_primary(sources)
        self._pass2_link(sources)
        self._pass3_unlinked(sources)
        self._rebuild_store_luts()
        return self._entity_store

    def _pass1_primary(self, sources: dict[str, dict[str, pd.DataFrame]]) -> None:
        create_foods_from_foodon(sources, self._entity_store, self._lut)
        create_chemicals_from_chebi(
            sources, self._entity_store, self._lut, self._corrections
        )
        create_diseases_from_ctd(sources, self._entity_store, self._lut)
        logger.info("Pass 1 complete: %d entities.", len(self._entity_store._entities))

    def _pass2_link(self, sources: dict[str, dict[str, pd.DataFrame]]) -> None:
        link_cdno_to_chebi(sources, self._entity_store)
        link_fdc_foods_to_foodon(
            sources, self._entity_store, self._corrections, self._linked_native_ids
        )
        link_fdc_nutrients(sources, self._entity_store, self._linked_native_ids)
        logger.info("Pass 2 complete: %d entities.", len(self._entity_store._entities))

    def _pass3_unlinked(self, sources: dict[str, dict[str, pd.DataFrame]]) -> None:
        create_unlinked_cdno(sources, self._entity_store, self._lut)
        create_unlinked_fdc_foods(
            sources, self._entity_store, self._lut, self._linked_native_ids
        )
        create_unlinked_fdc_nutrients(
            sources, self._entity_store, self._lut, self._linked_native_ids
        )
        logger.info("Pass 3 complete: %d entities.", len(self._entity_store._entities))

    def _rebuild_store_luts(self) -> None:
        self._entity_store._lut_food = self._lut.get_food_lut()
        self._entity_store._lut_chemical = self._lut.get_chemical_lut()
