"""Build triplets from resolved entities and Phase 1 edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .ambiguity import AmbiguityReport, build_ambiguity_from_extractions
from .chemical_chemical import merge_chemical_ontology
from .chemical_disease import merge_ctd_triplets
from .disease_disease import merge_disease_ontology
from .food_chemical import merge_fdc_triplets
from .food_food import merge_food_ontology

if TYPE_CHECKING:
    import pandas as pd

    from ..knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def build_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> AmbiguityReport:
    """Orchestrate triplet creation from resolved entities + Phase 1 edges.

    Mutates *kg* in memory. The caller is responsible for calling
    ``kg.save()`` after all triplet operations are complete.
    """
    merge_food_ontology(kg, sources)
    merge_chemical_ontology(kg, sources)
    merge_disease_ontology(kg, sources)
    merge_fdc_triplets(kg, sources)
    merge_ctd_triplets(kg, sources)

    report = build_ambiguity_from_extractions(kg.extractions)
    logger.info("Triplet build complete.")
    return report
