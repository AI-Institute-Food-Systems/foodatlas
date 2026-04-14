"""Pipeline stage enum for knowledge graph construction."""

from enum import Enum


class PipelineStage(Enum):
    """Ordered pipeline stages.

    Phase 1 (Ingest): Faithful source parsing into standardized parquet.
    Phase 2 (Construct): Filtering, entity resolution, triplet building.
    """

    INGEST = 0
    ENTITIES = 1
    TRIPLETS = 2
    IE = 3
    ENRICHMENT = 4


ALL_STAGES: list[PipelineStage] = sorted(PipelineStage, key=lambda s: s.value)
