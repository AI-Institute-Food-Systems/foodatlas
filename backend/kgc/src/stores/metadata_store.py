"""MetadataContainsStore — runtime container wrapping a pandas DataFrame."""

import logging
from pathlib import Path

import pandas as pd

from ..utils.json_io import read_json, write_json
from .schema import (
    FILE_METADATA_CONTAINS,
    INDEX_COL,
    METADATA_CONTAINS_COLUMNS,
)

logger = logging.getLogger(__name__)

COLUMNS = METADATA_CONTAINS_COLUMNS
FAID_PREFIX = "mc"


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
        records = read_json(self.path_metadata_contains)
        self._records = pd.DataFrame(records)
        if not self._records.empty:
            self._records = self._records.set_index(INDEX_COL)

        if self._records.empty:
            self._curr_mcid = 1
        else:
            max_mcid = self._records.index.str.slice(2).astype(int).max()
            self._curr_mcid = max_mcid + 1 if pd.notna(max_mcid) else 1

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        records = self._records.reset_index().to_dict(orient="records")
        write_json(path_output_dir / FILE_METADATA_CONTAINS, records)

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
