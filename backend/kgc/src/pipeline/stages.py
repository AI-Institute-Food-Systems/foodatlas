"""Pipeline stage enum for two-phase knowledge graph construction."""

from enum import Enum


class PipelineStage(Enum):
    """Ordered pipeline stages.

    Phase 1 (Ingest): Faithful source parsing into standardized parquet.
    Phase 2 (Adapt): Corrections, filtering, entity resolution, triplets.
    """

    INGEST = 0
    CORRECTIONS = 1
    SUBTREE_FILTER = 2
    ENTITY_RESOLUTION = 3
    TRIPLET_BUILD = 4
    METADATA_PROCESSING = 5
    TRIPLET_EXPANSION = 6
    POSTPROCESSING = 7


ALL_STAGES: list[PipelineStage] = sorted(PipelineStage, key=lambda s: s.value)

# Stages that belong to Phase 2 (construct layer).
ADAPT_STAGES = {
    PipelineStage.CORRECTIONS,
    PipelineStage.SUBTREE_FILTER,
    PipelineStage.ENTITY_RESOLUTION,
    PipelineStage.TRIPLET_BUILD,
    PipelineStage.POSTPROCESSING,
}
