"""Build triplets from resolved entities and Phase 1 edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ...utils.timing import log_duration
from .chemical_chemical import merge_chemical_ontology
from .chemical_disease import merge_ctd_triplets
from .disease_disease import merge_disease_ontology
from .food_chemical import merge_dmd_triplets, merge_fdc_triplets
from .food_food import merge_food_ontology

if TYPE_CHECKING:
    import pandas as pd

    from ..knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def build_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Orchestrate triplet creation from resolved entities + Phase 1 edges.

    Mutates *kg* in memory. The caller is responsible for calling
    ``kg.save()`` after all triplet operations are complete.
    """
    with log_duration("Food ontology triplets", logger):
        merge_food_ontology(kg, sources)
    with log_duration("Chemical ontology triplets", logger):
        merge_chemical_ontology(kg, sources)
    with log_duration("Disease ontology triplets", logger):
        merge_disease_ontology(kg, sources)
    with log_duration("FDC food-chemical triplets", logger):
        merge_fdc_triplets(kg, sources)
    with log_duration("CTD chemical-disease triplets", logger):
        merge_ctd_triplets(kg, sources)
    with log_duration("DMD food-chemical triplets", logger):
        merge_dmd_triplets(kg, sources)
    logger.info("Triplet build complete.")
