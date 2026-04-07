"""Pipeline stage enum for information extraction."""

from enum import Enum


class IEStage(Enum):
    """Ordered IE pipeline stages."""

    DOWNLOAD_PMC_IDS = 0
    UPDATE_BIOC = 1
    SEARCH_PUBMED = 2
    BIOBERT_FILTER = 3
    AGGREGATE = 4
    EXTRACT = 5
    PARSE = 6


ALL_STAGES: list[IEStage] = sorted(IEStage, key=lambda s: s.value)
