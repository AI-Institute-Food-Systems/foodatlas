"""KnowledgeGraph — orchestrates entity creation, triplet expansion, and queries."""

import logging
from pathlib import Path

import pandas as pd
from pympler import asizeof

from ..models.relationship import RelationshipType
from ..models.settings import KGCSettings
from ..stores.attestation_store import AttestationStore
from ..stores.entity_store import EntityStore
from ..stores.evidence_store import EvidenceStore
from ..stores.schema import (
    FILE_ATTESTATIONS,
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_TRIPLETS,
)
from ..stores.triplet_store import TripletStore
from ..utils.timing import log_duration

logger = logging.getLogger(__name__)


class KnowledgeGraph:
    """Central class tying together entities, triplets, evidence, and attestations.

    Args:
        settings: KGCSettings instance providing ``kg_dir`` and ``cache_dir``.
    """

    def __init__(self, settings: KGCSettings) -> None:
        self.settings = settings
        self._kg_dir = Path(settings.kg_dir)
        self._cache_dir = Path(settings.cache_dir) if settings.cache_dir else None

        self.evidence: EvidenceStore
        self.attestations: AttestationStore
        self.triplets: TripletStore
        self.entities: EntityStore

        self._load()
        with log_duration("Compute KG memory stats", logger):
            self.print_stats()

    def _load(self) -> None:
        """Load all KG stores from ``self._kg_dir``."""
        logger.info("Start loading the knowledge graph...")

        with log_duration("Load evidence store", logger):
            self.evidence = EvidenceStore(path=self._kg_dir / FILE_EVIDENCE)
        with log_duration("Load attestation store", logger):
            self.attestations = AttestationStore(path=self._kg_dir / FILE_ATTESTATIONS)
        with log_duration("Load triplet store", logger):
            self.triplets = TripletStore(path_triplets=self._kg_dir / FILE_TRIPLETS)
        with log_duration("Load entity store", logger):
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
        with log_duration("Save evidence", logger):
            self.evidence.save(out)
        with log_duration("Save attestations", logger):
            self.attestations.save(out)
        with log_duration("Save triplets", logger):
            self.triplets.save(out)
        with log_duration("Save entities", logger):
            self.entities.save(out)
        logger.info("Save complete.")

    def print_stats(self) -> None:
        """Log the memory footprint of the knowledge graph."""
        size_mb = asizeof.asizeof(self) / 1024 / 1024
        logger.info("KG space consumption: %.2f MB", size_mb)

    def add_triplets_from_resolved_ie(self, resolved: pd.DataFrame) -> int:
        """Add CONTAINS triplets from IE data with pre-resolved entity IDs.

        Args:
            resolved: DataFrame with evidence/attestation columns plus
                ``head_id`` and ``tail_id`` (already resolved and exploded).

        Returns:
            Number of new triplets created.
        """
        ev_df = resolved[["source_type", "reference"]].copy()
        ev_result = self.evidence.create(ev_df)

        att_df = resolved.copy()
        att_df["evidence_id"] = ev_result.index
        attestations = self.attestations.create(att_df)

        triplet_input = resolved[["head_id", "tail_id"]].copy()
        triplet_input.index = attestations.index
        triplet_input["relationship_id"] = RelationshipType.CONTAINS

        triplets = self.triplets.create(triplet_input)
        logger.info(
            "IE: %d attestations, %d new triplets.",
            len(attestations),
            len(triplets),
        )
        return len(triplets)

    def get_triplets(
        self,
        head_id: str | None = None,
        tail_id: str | None = None,
    ) -> pd.DataFrame:
        """Query triplets with optional head/tail filters.

        Returns a DataFrame of attestation rows annotated with ``triplet_key``.
        """
        filtered = self.triplets.filter(head_id=head_id, tail_id=tail_id)

        attestation_dfs: list[pd.DataFrame] = []
        for triplet_key, row in filtered.iterrows():
            att_ids = row.get("attestation_ids") or []
            if not att_ids:
                continue
            att_df = self.attestations.get(att_ids).copy()
            att_df["triplet_key"] = triplet_key
            attestation_dfs.append(att_df)

        return pd.concat(attestation_dfs) if attestation_dfs else pd.DataFrame()
