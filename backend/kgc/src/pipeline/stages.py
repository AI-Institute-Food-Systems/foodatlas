"""Pipeline stage enum mapping shell scripts to Python functions."""

from enum import Enum


class PipelineStage(Enum):
    """Ordered pipeline stages for knowledge graph construction.

    Each member maps to one of the original numbered shell scripts.
    The ``value`` is the execution order used for sorting.
    """

    PREPROCESSING = 0  # scripts/00_run_data_processing.sh
    KG_INIT = 1  # scripts/0_run_kg_init.sh
    METADATA_PROCESSING = 2  # scripts/1_run_metadata_processing.sh
    TRIPLET_EXPANSION = 3  # scripts/2_run_adding_triplets_from_metadata.sh
    POSTPROCESSING = 4  # scripts/3_run_postprocessing.sh


ALL_STAGES: list[PipelineStage] = sorted(PipelineStage, key=lambda s: s.value)
