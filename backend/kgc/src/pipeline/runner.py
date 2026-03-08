"""Pipeline runner — orchestrates KGC stages in order."""

from __future__ import annotations

import datetime
import hashlib
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ..constructor.knowledge_graph import KnowledgeGraph
from ..integration.data_cleaning.cdno import process_cdno
from ..integration.data_cleaning.chebi import process_chebi
from ..integration.data_cleaning.ctd import process_ctd
from ..integration.data_cleaning.flavordb import process_flavordb
from ..integration.data_cleaning.foodon import process_foodon
from ..integration.data_cleaning.mesh import process_mesh
from ..integration.data_cleaning.pubchem import process_pubchem
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
from ..integration.scaffold import create_empty_files
from ..integration.triplets.chemical_chemical.chebi import create_chemical_ontology
from ..integration.triplets.chemical_disease.ctd import merge_ctd_triplets
from ..integration.triplets.chemical_flavor.flavordb import merge_flavordb_triplets
from ..integration.triplets.food_chemical.fdc import merge_fdc
from ..integration.triplets.food_food.foodon import create_food_ontology
from ..postprocessing.common_name import apply_common_names
from ..postprocessing.grouping.chemicals import (
    generate_chemical_groups_cdno,
    generate_chemical_groups_chebi,
)
from ..postprocessing.grouping.foods import generate_food_groups_foodon
from ..postprocessing.synonyms_display import apply_synonyms_display
from ..utils.json_io import read_json, write_json
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

    def _run_data_cleaning(self) -> None:
        s = self._settings
        processors = [
            process_foodon,
            process_chebi,
            process_cdno,
            process_mesh,
            process_pubchem,
            process_ctd,
            process_flavordb,
        ]
        logger.info("Launching %d processors in parallel.", len(processors))
        with ProcessPoolExecutor(max_workers=len(processors)) as pool:
            futures = {pool.submit(fn, s): fn.__name__ for fn in processors}
            for future in as_completed(futures):
                name = futures[future]
                future.result()  # re-raises any exception
                logger.info("Processor %s finished.", name)

    def _run_kg_init(self) -> None:
        s = self._settings
        create_empty_files(s)

        kg = self._ensure_kg()
        append_foods_from_foodon(kg.entities, s)
        append_foods_from_fdc(kg.entities, s)
        append_chemicals_from_chebi(kg.entities, s)
        append_chemicals_from_cdno(kg.entities, s)
        append_chemicals_from_fdc(kg.entities, s)
        append_diseases_from_ctd(kg.entities, s)
        append_flavors_from_flavordb(kg.entities, s)

        create_food_ontology(kg.entities, s)
        create_chemical_ontology(kg.entities, s)

        merge_fdc(kg, s)
        merge_ctd_triplets(kg, s)
        merge_flavordb_triplets(kg, s)
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
        metadata = pd.DataFrame(read_json(metadata_path))
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
        write_json(version_path, version_info)
        logger.info("Wrote %s", version_path)


_STAGE_HANDLERS: dict[PipelineStage, Callable[[PipelineRunner], None]] = {
    PipelineStage.DATA_CLEANING: PipelineRunner._run_data_cleaning,
    PipelineStage.KG_INIT: PipelineRunner._run_kg_init,
    PipelineStage.METADATA_PROCESSING: PipelineRunner._run_metadata_processing,
    PipelineStage.TRIPLET_EXPANSION: PipelineRunner._run_triplet_expansion,
    PipelineStage.POSTPROCESSING: PipelineRunner._run_postprocessing,
}
