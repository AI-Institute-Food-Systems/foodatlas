"""EvidenceStore — immutable evidence records with content-addressed IDs."""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING

import pandas as pd

from .schema import EVIDENCE_COLUMNS, FILE_EVIDENCE

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

FAID_PREFIX = "ev"


def evidence_id(source_type: str, reference: str) -> str:
    """Deterministic ID from evidence content."""
    key = f"{source_type}:{reference}"
    return f"{FAID_PREFIX}{hashlib.sha256(key.encode()).hexdigest()[:12]}"


class EvidenceStore:
    """Manages immutable evidence records (one per source reference)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._records: pd.DataFrame = pd.DataFrame()
        self._seen: set[str] = set()
        self._load()

    def _load(self) -> None:
        if self._path.exists() and self._path.stat().st_size > 0:
            self._records = pd.read_parquet(self._path)
            if "evidence_id" in self._records.columns:
                self._records = self._records.set_index("evidence_id")
            self._seen = set(self._records.index)

    def save(self, path_output_dir: Path) -> None:
        df = self._records.reset_index()
        df.to_parquet(path_output_dir / FILE_EVIDENCE, index=False)

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        """Add evidence records, deduplicating by evidence_id.

        Expects columns: ``source_type``, ``reference``.
        Returns the full DataFrame (including previously seen rows)
        with ``evidence_id`` as index.
        """
        rows = rows.copy()
        rows["evidence_id"] = rows.apply(
            lambda r: evidence_id(str(r["source_type"]), str(r["reference"])),
            axis=1,
        )
        new = rows[~rows["evidence_id"].isin(self._seen)]
        if not new.empty:
            new = new.drop_duplicates(subset=["evidence_id"])
            new = new[EVIDENCE_COLUMNS].set_index("evidence_id")
            self._records = pd.concat([self._records, new])
            self._seen.update(new.index)

        return rows.set_index("evidence_id")[EVIDENCE_COLUMNS[1:]]

    def get(self, evidence_ids: list[str]) -> pd.DataFrame:
        """Retrieve evidence records by ID."""
        present = [eid for eid in evidence_ids if eid in self._seen]
        if not present:
            return pd.DataFrame()
        return self._records.loc[present]

    def __len__(self) -> int:
        return len(self._records)
