"""Pipeline stage enum mapping shell scripts to Python functions."""

from enum import Enum


class PipelineStage(Enum):
    """Ordered pipeline stages for knowledge graph construction.

    Each member maps to one of the original numbered shell scripts.
    The ``value`` is the execution order used for sorting.
    """

    DATA_CLEANING = 0  # data cleaning for external sources
    ENTITY_INIT = 1  # entity creation from external sources
    TRIPLET_INIT = 2  # triplet + ontology creation from external sources
    METADATA_PROCESSING = 3  # scripts/1_run_metadata_processing.sh
    TRIPLET_EXPANSION = 4  # scripts/2_run_adding_triplets_from_metadata.sh
    POSTPROCESSING = 5  # scripts/3_run_postprocessing.sh


ALL_STAGES: list[PipelineStage] = sorted(PipelineStage, key=lambda s: s.value)
