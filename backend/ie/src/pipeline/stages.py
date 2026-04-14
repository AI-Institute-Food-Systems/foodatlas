"""Pipeline stage enum for information extraction."""

from enum import Enum


class IEStage(Enum):
    """Ordered IE pipeline stages."""

    CORPUS = 0
    SEARCH = 1
    FILTERING = 2
    EXTRACTION = 3


ALL_STAGES: list[IEStage] = sorted(IEStage, key=lambda s: s.value)
