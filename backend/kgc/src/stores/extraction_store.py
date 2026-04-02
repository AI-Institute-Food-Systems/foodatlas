"""ExtractionStore — extraction records with content-addressed IDs."""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING

import pandas as pd

from .schema import EXTRACTION_COLUMNS, FILE_EXTRACTIONS

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

FAID_PREFIX = "ex"


def extraction_id(
    evidence_id: str, extractor: str, head_name: str, tail_name: str
) -> str:
    """Deterministic ID from extraction content."""
    key = f"{evidence_id}:{extractor}:{head_name}:{tail_name}"
    return f"{FAID_PREFIX}{hashlib.sha256(key.encode()).hexdigest()[:12]}"


class ExtractionStore:
    """Manages extraction records (one per evidence x extractor x result)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._records: pd.DataFrame = pd.DataFrame()
        self._load()

    def _load(self) -> None:
        if self._path.exists() and self._path.stat().st_size > 0:
            self._records = pd.read_parquet(self._path)
            if "extraction_id" in self._records.columns:
                self._records = self._records.set_index("extraction_id")

    def save(self, path_output_dir: Path) -> None:
        df = self._records.reset_index()
        df.to_parquet(path_output_dir / FILE_EXTRACTIONS, index=False)

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        """Add extraction records with content-addressed IDs.

        Expects columns matching :data:`EXTRACTION_COLUMNS` (minus
        ``extraction_id`` which is auto-generated).
        """
        rows = rows.copy()
        rows["extraction_id"] = rows.apply(
            lambda r: extraction_id(
                str(r.get("evidence_id", "")),
                str(r.get("extractor", "")),
                str(r.get("head_name_raw", "")),
                str(r.get("tail_name_raw", "")),
            ),
            axis=1,
        )
        rows = rows[EXTRACTION_COLUMNS].set_index("extraction_id")
        self._records = pd.concat([self._records, rows])
        return rows

    def get(self, extraction_ids: list[str]) -> pd.DataFrame:
        """Retrieve extraction records by ID."""
        present = [eid for eid in extraction_ids if eid in self._records.index]
        if not present:
            return pd.DataFrame()
        return self._records.loc[present]

    def __len__(self) -> int:
        return len(self._records)
