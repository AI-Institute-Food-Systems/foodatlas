"""ConstructRunner — orchestrates Phase 2 construct stages."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ..config.corrections import load_corrections
from ..constructor.knowledge_graph import KnowledgeGraph
from ..ingest.protocol import deserialize_raw_attrs
from ..integration.scaffold import create_empty_entity_files, create_empty_triplet_files
from ..postprocessing.common_name import apply_common_names
from ..postprocessing.grouping.chemicals import (
    generate_chemical_groups_cdno,
    generate_chemical_groups_chebi,
)
from ..postprocessing.grouping.foods import generate_food_groups_foodon
from ..postprocessing.synonyms_display import apply_synonyms_display
from .entity_resolver import EntityResolver
from .subtree_filter import filter_sources
from .triplet_builder import build_triplets

if TYPE_CHECKING:
    from ..models.settings import KGCSettings

logger = logging.getLogger(__name__)

_SOURCE_IDS = ["foodon", "chebi", "cdno", "ctd", "mesh", "pubchem", "flavordb", "fdc"]


class ConstructRunner:
    """Orchestrate Phase 2: corrections → filter → resolve → triplets → post."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(self) -> None:
        """Run all construct stages in order."""
        sources = self._load_ingest_output()
        corrections = load_corrections()

        # Corrections are disabled — the base KG should be a faithful
        # representation of the source data. All fixes are applied as
        # patches at read time (overlay/override pattern).

        logger.info("=== SUBTREE FILTER ===")
        filter_sources(sources, corrections.ontology_roots)

        logger.info("=== ENTITY RESOLUTION ===")
        kg_dir = Path(self._settings.kg_dir)
        create_empty_entity_files(self._settings)
        resolver = EntityResolver(kg_dir, corrections)
        resolver.resolve(sources)
        resolver.entity_store.save(kg_dir)

        logger.info("=== TRIPLET BUILD ===")
        create_empty_triplet_files(self._settings)
        kg = KnowledgeGraph(self._settings)
        build_triplets(kg, sources, self._settings)

        # Postprocessing (common names, grouping, synonym display) is
        # intentionally skipped — it operates on the final KG and can be
        # run separately once the base KG is validated.
        logger.info("Construct complete. Skipping postprocessing.")

    def _load_ingest_output(self) -> dict[str, dict[str, pd.DataFrame]]:
        """Load Phase 1 parquet artifacts into memory."""
        ingest_dir = Path(self._settings.ingest_dir)
        result: dict[str, dict[str, pd.DataFrame]] = {}

        for source_id in _SOURCE_IDS:
            source_dir = ingest_dir / source_id
            if not source_dir.exists():
                logger.warning("No ingest output for %s.", source_id)
                continue

            data: dict[str, pd.DataFrame] = {}
            for suffix in ("nodes", "edges", "xrefs"):
                path = source_dir / f"{source_id}_{suffix}.parquet"
                if path.exists():
                    df = pd.read_parquet(path)
                    data[suffix] = deserialize_raw_attrs(df)

            if data:
                result[source_id] = data

        logger.info("Loaded ingest output for %d sources.", len(result))
        return result

    def _postprocess(self, kg: KnowledgeGraph) -> None:
        s = self._settings
        store = kg.entities
        chemicals = store._entities.query("entity_type == 'chemical'")

        food_groups = generate_food_groups_foodon(store, s)
        store._entities.loc[food_groups.index, "foodon_group"] = food_groups

        cdno_groups = generate_chemical_groups_cdno(chemicals, s)
        store._entities.loc[cdno_groups.index, "cdno_group"] = cdno_groups

        chebi_groups = generate_chemical_groups_chebi(chemicals, store, s)
        store._entities.loc[chebi_groups.index, "chebi_group"] = chebi_groups

        apply_common_names(store, kg.triplets, kg.metadata)
        apply_synonyms_display(store)
        kg.save()
