"""Build triplets from resolved entities and Phase 1 edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .chemical_disease import merge_ctd_triplets
from .chemical_ontology import create_chemical_ontology
from .disease_ontology import create_disease_ontology
from .flavor import apply_flavor_descriptions
from .food_chemical import merge_fdc_triplets
from .food_ontology import create_food_ontology

if TYPE_CHECKING:
    import pandas as pd

    from .knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def build_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Orchestrate triplet creation from resolved entities + Phase 1 edges.

    Mutates *kg* in memory. The caller is responsible for calling
    ``kg.save()`` after all triplet operations are complete.
    """
    food_onto = create_food_ontology(kg.entities, sources)
    kg.triplets.add_ontology(food_onto)

    chem_onto = create_chemical_ontology(kg.entities, sources)
    kg.triplets.add_ontology(chem_onto)

    disease_onto = create_disease_ontology(kg.entities, sources)
    kg.triplets.add_ontology(disease_onto)

    merge_fdc_triplets(kg, sources)
    merge_ctd_triplets(kg, sources)
    apply_flavor_descriptions(kg, sources)
    logger.info("Triplet build complete.")
