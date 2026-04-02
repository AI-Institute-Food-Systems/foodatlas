"""MetadataContainsStore — runtime container wrapping a pandas DataFrame."""

import hashlib
import json
import logging
from pathlib import Path

import pandas as pd

from .schema import (
    FILE_METADATA_CONTAINS,
    INDEX_COL,
    METADATA_CONTAINS_COLUMNS,
)

logger = logging.getLogger(__name__)

COLUMNS = METADATA_CONTAINS_COLUMNS
FAID_PREFIX = "mc"


def _content_hash(food_name: str, chem_name: str, source: str, ref: str) -> str:
    """Deterministic ID from metadata content."""
    key = f"{food_name}:{chem_name}:{source}:{ref}"
    digest = hashlib.sha256(key.encode()).hexdigest()[:12]
    return f"{FAID_PREFIX}{digest}"


class MetadataContainsStore:
    """Manages metadata records for "contains" relationship triplets.

    IDs are content-addressed (deterministic hash of food name, chemical
    name, source, and reference) so the same evidence always gets the
    same ID regardless of creation order.
    """

    def __init__(self, path_metadata_contains: Path) -> None:
        self.path_metadata_contains = Path(path_metadata_contains)

        self._records: pd.DataFrame = pd.DataFrame()

        self._load()

    def _load(self) -> None:
        path = self.path_metadata_contains
        if path.exists() and path.stat().st_size > 0:
            self._records = pd.read_parquet(path)
            if INDEX_COL in self._records.columns:
                self._records = self._records.set_index(INDEX_COL)
            if "reference" in self._records.columns:
                self._records["reference"] = self._records["reference"].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )
        else:
            self._records = pd.DataFrame()

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        df = self._records.reset_index()
        if "reference" in df.columns:
            df["reference"] = df["reference"].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else x
            )
        df.to_parquet(path_output_dir / FILE_METADATA_CONTAINS, index=False)

    def create(self, metadata: pd.DataFrame) -> pd.DataFrame:
        """Add new metadata entries with content-addressed IDs."""
        metadata = metadata.reset_index(drop=True)

        ids = metadata.apply(
            lambda r: _content_hash(
                str(r.get("_food_name", "")),
                str(r.get("_chemical_name", "")),
                str(r.get("source", "")),
                str(r.get("reference", "")),
            ),
            axis=1,
        )
        metadata[INDEX_COL] = ids
        metadata = metadata[COLUMNS].set_index(INDEX_COL)

        self._records = pd.concat([self._records, metadata])
        return metadata

    def get(self, metadata_ids: list[str]) -> pd.DataFrame:
        return self._records.loc[metadata_ids]
