"""Build triplets from resolved entities and Phase 1 edges."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..postprocessing.flavor import apply_flavor_descriptions
from .ambiguity import AmbiguityReport, collect_ambiguity
from .chemical_chemical import (
    _build_chebi_to_fa_map,
    create_chemical_ontology,
)
from .chemical_disease import (
    _build_disease_to_fa,
    _build_mesh_to_fa,
    merge_ctd_triplets,
)
from .disease_disease import (
    _build_disease_to_fa_map,
    create_disease_ontology,
)
from .food_chemical import _build_fdc_maps, merge_fdc_triplets
from .food_food import _build_foodon_to_fa_map, create_food_ontology

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
    kg.triplets.add_ontology(create_food_ontology(kg.entities, sources))
    kg.triplets.add_ontology(create_chemical_ontology(kg.entities, sources))
    kg.triplets.add_ontology(create_disease_ontology(kg.entities, sources))
    merge_fdc_triplets(kg, sources)
    merge_ctd_triplets(kg, sources)
    apply_flavor_descriptions(kg, sources)

    report = _collect_all_ambiguity(kg)
    logger.info("Triplet build complete.")
    return report


def _collect_all_ambiguity(kg: KnowledgeGraph) -> AmbiguityReport:
    """Scan all ID maps for 1:N ambiguity after triplets are built."""
    ents = kg.entities._entities
    report = AmbiguityReport()

    report.records.extend(
        collect_ambiguity(_build_foodon_to_fa_map(kg.entities), ents, "food", "foodon")
    )
    report.records.extend(
        collect_ambiguity(
            _build_chebi_to_fa_map(kg.entities), ents, "chemical", "chebi"
        )
    )
    report.records.extend(
        collect_ambiguity(_build_disease_to_fa_map(kg.entities), ents, "disease", "ctd")
    )

    fdc2fa = _build_fdc_maps(ents)
    report.records.extend(collect_ambiguity(fdc2fa["food"], ents, "food", "fdc_food"))
    report.records.extend(
        collect_ambiguity(fdc2fa["nutrient"], ents, "chemical", "fdc_nutrient")
    )
    report.records.extend(
        collect_ambiguity(_build_mesh_to_fa(ents), ents, "chemical", "ctd_mesh")
    )
    report.records.extend(
        collect_ambiguity(_build_disease_to_fa(ents), ents, "disease", "ctd_disease")
    )
    return report
