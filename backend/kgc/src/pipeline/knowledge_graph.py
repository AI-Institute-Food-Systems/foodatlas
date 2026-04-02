"""KnowledgeGraph — orchestrates entity creation, triplet expansion, and queries."""

import logging
from pathlib import Path

import pandas as pd
from pympler import asizeof

from ..models.relationship import RelationshipType
from ..models.settings import KGCSettings
from ..stores.entity_store import EntityStore
from ..stores.evidence_store import EvidenceStore
from ..stores.extraction_store import ExtractionStore
from ..stores.schema import (
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_EXTRACTIONS,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_TRIPLETS,
)
from ..stores.triplet_store import TripletStore

logger = logging.getLogger(__name__)


class KnowledgeGraph:
    """Central class tying together entities, triplets, evidence, and extractions.

    Args:
        settings: KGCSettings instance providing ``kg_dir`` and ``cache_dir``.
    """

    def __init__(self, settings: KGCSettings) -> None:
        self.settings = settings
        self._kg_dir = Path(settings.kg_dir)
        self._cache_dir = Path(settings.cache_dir) if settings.cache_dir else None

        self.evidence: EvidenceStore
        self.extractions: ExtractionStore
        self.triplets: TripletStore
        self.entities: EntityStore

        self._load()
        self.print_stats()

    def _load(self) -> None:
        """Load all KG stores from ``self._kg_dir``."""
        logger.info("Start loading the knowledge graph...")

        self.evidence = EvidenceStore(path=self._kg_dir / FILE_EVIDENCE)
        self.extractions = ExtractionStore(path=self._kg_dir / FILE_EXTRACTIONS)
        self.triplets = TripletStore(path_triplets=self._kg_dir / FILE_TRIPLETS)
        self.entities = EntityStore(
            path_entities=self._kg_dir / FILE_ENTITIES,
            path_lut_food=self._kg_dir / FILE_LUT_FOOD,
            path_lut_chemical=self._kg_dir / FILE_LUT_CHEMICAL,
            path_kg=self._kg_dir,
            path_cache_dir=self._cache_dir,
        )

        logger.info("Completed loading the knowledge graph!")

    def save(self, path_output_dir: Path | None = None) -> None:
        """Save all KG stores to *path_output_dir* (defaults to kg_dir)."""
        out = Path(path_output_dir) if path_output_dir else self._kg_dir
        self.evidence.save(out)
        self.extractions.save(out)
        self.triplets.save(out)
        self.entities.save(out)

    def print_stats(self) -> None:
        """Log the memory footprint of the knowledge graph."""
        size_mb = asizeof.asizeof(self) / 1024 / 1024
        logger.info("KG space consumption: %.2f MB", size_mb)

    def add_triplets_from_resolved_ie(self, resolved: pd.DataFrame) -> int:
        """Add CONTAINS triplets from IE data with pre-resolved entity IDs.

        Args:
            resolved: DataFrame with evidence/extraction columns plus
                ``head_id`` and ``tail_id`` (already resolved and exploded).

        Returns:
            Number of new triplets created.
        """
        ev_df = resolved[["source_type", "reference"]].copy()
        ev_result = self.evidence.create(ev_df)

        ex_df = resolved.copy()
        ex_df["evidence_id"] = ev_result.index
        extractions = self.extractions.create(ex_df)

        triplet_input = resolved[["head_id", "tail_id"]].copy()
        triplet_input.index = extractions.index
        triplet_input["relationship_id"] = RelationshipType.CONTAINS

        triplets = self.triplets.create(triplet_input)
        logger.info(
            "IE: %d extractions, %d new triplets.",
            len(extractions),
            len(triplets),
        )
        return len(triplets)

    def get_triplets(
        self,
        head_id: str | None = None,
        tail_id: str | None = None,
    ) -> pd.DataFrame:
        """Query triplets with optional head/tail filters.

        Returns a DataFrame of extraction rows annotated with ``triplet_key``.
        """
        filtered = self.triplets.filter(head_id=head_id, tail_id=tail_id)

        extraction_dfs: list[pd.DataFrame] = []
        for triplet_key, row in filtered.iterrows():
            ext_ids = row.get("extraction_ids") or []
            if not ext_ids:
                continue
            ext_df = self.extractions.get(ext_ids).copy()
            ext_df["triplet_key"] = triplet_key
            extraction_dfs.append(ext_df)

        return pd.concat(extraction_dfs) if extraction_dfs else pd.DataFrame()
