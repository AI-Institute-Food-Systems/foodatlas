"""Pipeline runner — orchestrates KGC stages in order."""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ..constructor.knowledge_graph import KnowledgeGraph
from ..integration.entities.chemical.init_entities import (
    append_chemicals_from_cdno,
    append_chemicals_from_chebi,
    append_chemicals_from_fdc,
)
from ..integration.entities.disease.init_entities import append_diseases_from_ctd
from ..integration.entities.flavor.init_entities import append_flavors_from_flavordb
from ..integration.entities.food.init_entities import (
    append_foods_from_fdc,
    append_foods_from_foodon,
)
from ..integration.ontologies.cdno import process_cdno
from ..integration.ontologies.chebi import process_chebi
from ..integration.ontologies.chemical import create_chemical_ontology
from ..integration.ontologies.food import create_food_ontology
from ..integration.ontologies.foodon import process_foodon
from ..integration.ontologies.mesh import process_mesh
from ..integration.ontologies.pubchem import process_pubchem
from ..integration.scaffold import create_empty_files
from ..integration.triplets.ctd import merge_ctd_triplets
from ..integration.triplets.fdc import merge_fdc
from ..integration.triplets.flavordb import merge_flavordb_triplets
from ..postprocessing.common_name import apply_common_names
from ..postprocessing.grouping.chemicals import (
    generate_chemical_groups_cdno,
    generate_chemical_groups_chebi,
)
from ..postprocessing.grouping.foods import generate_food_groups_foodon
from ..postprocessing.synonyms_display import apply_synonyms_display
from .stages import ALL_STAGES, PipelineStage

if TYPE_CHECKING:
    from collections.abc import Callable

    from ..models.settings import KGCSettings

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Run KGC pipeline stages in order.

    Args:
        settings: KGCSettings instance with all paths and credentials.
    """

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings
        self._kg: KnowledgeGraph | None = None

    def run(self, stages: list[PipelineStage] | None = None) -> None:
        """Run all or selected stages in order."""
        to_run = sorted(stages or ALL_STAGES, key=lambda s: s.value)

        logger.info("Pipeline starting — stages: %s", [s.name for s in to_run])
        t0 = time.monotonic()

        for stage in to_run:
            self.run_stage(stage)

        elapsed = time.monotonic() - t0
        logger.info("Pipeline finished in %.1fs", elapsed)

        if stages is None:
            self._write_version()

    def run_stage(self, stage: PipelineStage) -> None:
        """Run a single stage, logging start/end/duration."""
        logger.info("=== Stage %s START ===", stage.name)
        t0 = time.monotonic()

        _STAGE_HANDLERS[stage](self)

        elapsed = time.monotonic() - t0
        logger.info("=== Stage %s END (%.1fs) ===", stage.name, elapsed)

    def _ensure_kg(self) -> KnowledgeGraph:
        """Lazily load the KnowledgeGraph."""
        if self._kg is None:
            self._kg = KnowledgeGraph(self._settings)
        return self._kg

    # ------------------------------------------------------------------
    # Stage handlers
    # ------------------------------------------------------------------

    def _run_ontology_prep(self) -> None:
        s = self._settings
        process_foodon(s)
        process_chebi(s)
        process_cdno(s)
        process_mesh(s)
        process_pubchem(s)

    def _run_kg_init(self) -> None:
        s = self._settings
        create_empty_files(s)

        kg = self._ensure_kg()
        append_foods_from_foodon(kg.entities, s)
        append_foods_from_fdc(kg.entities, s)
        append_chemicals_from_chebi(kg.entities, s)
        append_chemicals_from_cdno(kg.entities, s)
        append_chemicals_from_fdc(kg.entities, s)

        create_food_ontology(kg.entities, s)
        create_chemical_ontology(kg.entities, s)

        merge_fdc(kg, s)
        kg.save()

    def _run_metadata_processing(self) -> None:
        logger.info("Metadata processing is handled by the IE pipeline.")

    def _run_triplet_expansion(self) -> None:
        kg_dir = Path(self._settings.kg_dir)
        metadata_path = kg_dir / "_metadata_new.json"
        if not metadata_path.exists():
            logger.warning("No metadata at %s — skipping.", metadata_path)
            return

        kg = self._ensure_kg()
        metadata = pd.read_json(metadata_path, orient="records")
        kg.add_triplets_from_metadata(metadata)
        kg.save()

        logger.info("Triplet expansion complete — running validation.")
        self._validate_kg()

    def _run_postprocessing(self) -> None:
        kg = self._ensure_kg()
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

    def _run_merge_disease(self) -> None:
        kg = self._ensure_kg()
        s = self._settings
        append_diseases_from_ctd(kg, s)
        merge_ctd_triplets(kg, s)
        kg.save()

    def _run_merge_flavor(self) -> None:
        kg = self._ensure_kg()
        s = self._settings
        append_flavors_from_flavordb(kg, s)
        merge_flavordb_triplets(kg, s)
        kg.save()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_kg(self) -> None:
        """Basic validation after triplet expansion."""
        kg = self._ensure_kg()
        n_ent = len(kg.entities._entities)
        n_tri = len(kg.triplets._triplets)
        logger.info("Validation — entities: %d, triplets: %d", n_ent, n_tri)
        if n_ent == 0:
            logger.warning("KG has zero entities after triplet expansion.")
        if n_tri == 0:
            logger.warning("KG has zero triplets after triplet expansion.")

    def _write_version(self) -> None:
        """Write version.json after a full pipeline run."""
        version_info: dict[str, object] = {
            "timestamp": datetime.datetime.now(tz=datetime.UTC).isoformat(),
            "stages": [s.name for s in ALL_STAGES],
            "version": "0.1.0",
        }

        kg_dir = Path(self._settings.kg_dir)
        entities_path = kg_dir / "entities.json"
        if entities_path.exists():
            h = hashlib.sha256(entities_path.read_bytes()).hexdigest()[:12]
            version_info["entities_hash"] = h

        version_path = kg_dir / "version.json"
        with version_path.open("w") as f:
            json.dump(version_info, f, indent=2)
        logger.info("Wrote %s", version_path)


_STAGE_HANDLERS: dict[PipelineStage, Callable[[PipelineRunner], None]] = {
    PipelineStage.ONTOLOGY_PREP: PipelineRunner._run_ontology_prep,
    PipelineStage.KG_INIT: PipelineRunner._run_kg_init,
    PipelineStage.METADATA_PROCESSING: PipelineRunner._run_metadata_processing,
    PipelineStage.TRIPLET_EXPANSION: PipelineRunner._run_triplet_expansion,
    PipelineStage.POSTPROCESSING: PipelineRunner._run_postprocessing,
    PipelineStage.MERGE_DISEASE: PipelineRunner._run_merge_disease,
    PipelineStage.MERGE_FLAVOR: PipelineRunner._run_merge_flavor,
}
