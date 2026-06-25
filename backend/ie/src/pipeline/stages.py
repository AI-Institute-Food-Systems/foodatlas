"""Pipeline stage enum for information extraction."""

from enum import Enum


class IEStage(Enum):
    """Ordered IE pipeline stages."""

    SEARCH = 1
    RETRIEVAL = 2
    FILTERING = 3
    EXTRACTION = 4


ALL_STAGES: list[IEStage] = sorted(IEStage, key=lambda s: s.value)
