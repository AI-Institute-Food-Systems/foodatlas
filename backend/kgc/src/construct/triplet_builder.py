"""Build triplets from resolved entities and Phase 1 edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .triplets.chemical_ontology import create_chemical_ontology
from .triplets.flavor import apply_flavor_descriptions
from .triplets.food_chemical import merge_fdc_triplets
from .triplets.food_ontology import create_food_ontology

if TYPE_CHECKING:
    import pandas as pd

    from ..constructor.knowledge_graph import KnowledgeGraph
    from ..models.settings import KGCSettings

logger = logging.getLogger(__name__)


def build_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
    settings: KGCSettings,
) -> None:
    """Orchestrate triplet creation from resolved entities + Phase 1 edges.

    Mutates *kg* in memory. The caller is responsible for calling
    ``kg.save()`` after all triplet operations are complete.
    """
    create_food_ontology(kg.entities, sources, settings)
    create_chemical_ontology(kg.entities, sources, settings)
    merge_fdc_triplets(kg, sources)
    apply_flavor_descriptions(kg, sources)
    logger.info("Triplet build complete.")
