"""KnowledgeGraph — orchestrates entity creation, triplet expansion, and queries."""

import logging
from pathlib import Path

import pandas as pd
from pympler import asizeof

from ...models.relationship import RelationshipType
from ...models.settings import KGCSettings
from ...stores.entity_store import EntityStore
from ...stores.metadata_store import MetadataContainsStore
from ...stores.schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_METADATA_CONTAINS,
    FILE_TRIPLETS,
)
from ...stores.triplet_store import TripletStore

logger = logging.getLogger(__name__)


class KnowledgeGraph:
    """Central class tying together entities, triplets, and metadata.

    Args:
        settings: KGCSettings instance providing ``kg_dir`` and ``cache_dir``.
    """

    def __init__(self, settings: KGCSettings) -> None:
        self.settings = settings
        self._kg_dir = Path(settings.kg_dir)
        self._cache_dir = Path(settings.cache_dir) if settings.cache_dir else None

        self.metadata: MetadataContainsStore
        self.triplets: TripletStore
        self.entities: EntityStore

        self._load()
        self.print_stats()

    def _load(self) -> None:
        """Load all KG stores from ``self._kg_dir``."""
        logger.info("Start loading the knowledge graph...")

        self.metadata = MetadataContainsStore(
            path_metadata_contains=self._kg_dir / FILE_METADATA_CONTAINS,
        )
        self.triplets = TripletStore(
            path_triplets=self._kg_dir / FILE_TRIPLETS,
        )
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
        self.metadata.save(out)
        self.triplets.save(out)
        self.entities.save(out)

    def print_stats(self) -> None:
        """Log the memory footprint of the knowledge graph."""
        size_mb = asizeof.asizeof(self) / 1024 / 1024
        logger.info("KG space consumption: %.2f MB", size_mb)

    def add_triplets_from_metadata(
        self,
        metadata: pd.DataFrame,
        relationship_type: str = "contains",
    ) -> None:
        """Add triplets from a metadata DataFrame.

        Args:
            metadata: DataFrame with ``_food_name`` and ``_chemical_name``.
            relationship_type: Only ``"contains"`` is supported.
        """
        if relationship_type == "contains":
            self._add_triplets_from_metadata_contains(metadata)
        else:
            msg = f"Unsupported relationship type: {relationship_type}"
            raise NotImplementedError(msg)

    def _add_triplets_from_metadata_contains(
        self,
        metadata: pd.DataFrame,
    ) -> None:
        """Build metadata + triplets from pre-resolved entity names.

        Expects all food/chemical names in the metadata to already exist
        in the entity store. Unknown names are silently dropped.
        """
        metadata = self.metadata.create(metadata)

        exploded = metadata.copy()
        exploded["head_id"] = exploded["_food_name"].apply(
            lambda x: self.entities.get_entity_ids("food", x)
        )
        exploded["tail_id"] = exploded["_chemical_name"].apply(
            lambda x: self.entities.get_entity_ids("chemical", x)
        )
        exploded = exploded.explode("head_id").explode("tail_id")
        exploded = exploded.dropna(subset=["head_id", "tail_id"])
        exploded["relationship_id"] = RelationshipType.CONTAINS
        triplets = self.triplets.create(exploded)

        logger.info("# metadata entries added: %d", len(metadata))
        logger.info("# triplets added: %d", len(triplets))

    def get_triplets(
        self,
        head_id: str | None = None,
        tail_id: str | None = None,
    ) -> pd.DataFrame:
        """Query triplets with optional head/tail filters.

        Returns a DataFrame of metadata rows annotated with ``triplet_id``.
        """
        filtered = self.triplets.filter(head_id=head_id, tail_id=tail_id)

        metadata_dfs: list[pd.DataFrame] = []
        for triplet_id, row in filtered.iterrows():
            meta_df = self.metadata.get(row["metadata_ids"]).copy()
            meta_df["triplet_id"] = triplet_id
            metadata_dfs.append(meta_df)

        return pd.concat(metadata_dfs) if metadata_dfs else pd.DataFrame()
