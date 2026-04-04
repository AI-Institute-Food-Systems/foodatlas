"""AttestationStore — attestation records with content-addressed IDs."""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING

import pandas as pd
from pydantic_core import PydanticUndefined

from ..models.attestation import Attestation
from .schema import ATTESTATION_COLUMNS, FILE_ATTESTATIONS

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

FAID_PREFIX = "at"


def attestation_id(
    evidence_id: str, source: str, head_name: str, tail_name: str
) -> str:
    """Deterministic ID from attestation content."""
    key = f"{evidence_id}:{source}:{head_name}:{tail_name}"
    return f"{FAID_PREFIX}{hashlib.sha256(key.encode()).hexdigest()[:12]}"


class AttestationStore:
    """Manages attestation records (one per evidence x source x result)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._records: pd.DataFrame = pd.DataFrame()
        self._seen: set[str] = set()
        self._load()

    def _load(self) -> None:
        if self._path.exists() and self._path.stat().st_size > 0:
            self._records = pd.read_parquet(self._path)
            if "attestation_id" in self._records.columns:
                self._records = self._records.set_index("attestation_id")
            self._seen = set(self._records.index)

    def save(self, path_output_dir: Path) -> None:
        df = self._records.reset_index()
        df.to_parquet(path_output_dir / FILE_ATTESTATIONS, index=False)

    def create(self, rows: pd.DataFrame) -> pd.DataFrame:
        """Add attestation records with content-addressed IDs.

        Expects columns matching :data:`ATTESTATION_COLUMNS` (minus
        ``attestation_id`` which is auto-generated).
        """
        rows = rows.copy()
        for col, field in Attestation.model_fields.items():
            if col in rows.columns:
                continue
            if field.default_factory is not None:
                rows[col] = [field.default_factory() for _ in range(len(rows))]
            elif field.default is not PydanticUndefined:
                rows[col] = field.default
        keys = (
            rows["evidence_id"].astype(str)
            + ":"
            + rows["source"].astype(str)
            + ":"
            + rows["head_name_raw"].astype(str)
            + ":"
            + rows["tail_name_raw"].astype(str)
        )
        rows["attestation_id"] = FAID_PREFIX + keys.apply(
            lambda k: hashlib.sha256(k.encode()).hexdigest()[:12]
        )
        rows = rows[ATTESTATION_COLUMNS].set_index("attestation_id")
        new = rows[~rows.index.isin(self._seen)]
        if not new.empty:
            new = new[~new.index.duplicated(keep="first")]
            self._records = pd.concat([self._records, new])
            self._seen.update(new.index)
        return rows

    def get(self, attestation_ids: list[str]) -> pd.DataFrame:
        """Retrieve attestation records by ID."""
        present = [eid for eid in attestation_ids if eid in self._records.index]
        if not present:
            return pd.DataFrame()
        return self._records.loc[present]

    def __len__(self) -> int:
        return len(self._records)
