"""MetadataContainsStore — runtime container wrapping a pandas DataFrame."""

import logging
from ast import literal_eval
from pathlib import Path

import pandas as pd

from .schema import (
    FILE_METADATA_CONTAINS,
    INDEX_COL,
    METADATA_CONTAINS_COLUMNS,
    TSV_SEP,
)

logger = logging.getLogger(__name__)

COLUMNS = METADATA_CONTAINS_COLUMNS
FAID_PREFIX = "mc"


def _nan_to_empty(value: object) -> str:
    """Convert NaN values to empty strings when loading TSV."""
    return "" if pd.isna(value) else str(value)


class MetadataContainsStore:
    """Manages metadata records for "contains" relationship triplets.

    Stores concentration, food parts, processing, sources, and quality
    scores with auto-generated IDs (prefix "mc").
    """

    def __init__(self, path_metadata_contains: Path) -> None:
        self.path_metadata_contains = Path(path_metadata_contains)

        self._records: pd.DataFrame = pd.DataFrame()
        self._curr_mcid: int = 1

        self._load()

    def _load(self) -> None:
        self._records = pd.read_csv(
            self.path_metadata_contains,
            sep=TSV_SEP,
            converters={
                "reference": literal_eval,
                "_conc": _nan_to_empty,
                "_food_part": _nan_to_empty,
            },
        ).set_index(INDEX_COL)

        max_mcid = self._records.index.str.slice(2).astype(int).max()
        self._curr_mcid = max_mcid + 1 if pd.notna(max_mcid) else 1

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        self._records.to_csv(path_output_dir / FILE_METADATA_CONTAINS, sep=TSV_SEP)

    def create(self, metadata: pd.DataFrame) -> pd.DataFrame:
        """Add new metadata entries with auto-generated IDs."""
        metadata = metadata.reset_index(drop=True)
        metadata[INDEX_COL] = FAID_PREFIX + (self._curr_mcid + metadata.index).astype(
            str
        )
        metadata = metadata[COLUMNS].set_index(INDEX_COL)

        self._curr_mcid += len(metadata)
        self._records = pd.concat([self._records, metadata])
        return metadata

    def get(self, metadata_ids: list[str]) -> pd.DataFrame:
        return self._records.loc[metadata_ids]
