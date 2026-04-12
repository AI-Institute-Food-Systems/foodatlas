"""EntityRegistry — persistent (source, native_id) → foodatlas_id mapping."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from .schema import REGISTRY_COLUMNS

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

FAID_PREFIX = "e"


class EntityRegistry:
    """Persistent mapping from (source, native_id) to foodatlas_id(s).

    Supports 1:N mappings — a single (source, native_id) can resolve to
    multiple entities (e.g. stereoisomers sharing the same external ID).
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._forward: dict[tuple[str, str], list[str]] = {}
        self._max_eid: int = 0
        self._load()

    def _load(self) -> None:
        if self._path.exists() and self._path.stat().st_size > 0:
            df = pd.read_parquet(self._path)
            for _, row in df.iterrows():
                key = (str(row["source"]), str(row["native_id"]))
                fid = str(row["foodatlas_id"])
                self._forward.setdefault(key, []).append(fid)
            self._recompute_max_eid()
        logger.info("Registry loaded: %d keys, max eid=%d.", len(self), self._max_eid)

    def _recompute_max_eid(self) -> None:
        if not self._forward:
            self._max_eid = 0
            return
        self._max_eid = max(
            int(fid[len(FAID_PREFIX) :])
            for ids in self._forward.values()
            for fid in ids
            if fid.startswith(FAID_PREFIX)
        )

    @property
    def next_eid(self) -> int:
        """Next available entity ID number."""
        return self._max_eid + 1

    def resolve(self, source: str, native_id: str) -> list[str]:
        """Look up foodatlas_ids for *(source, native_id)*.

        Returns a list of foodatlas_ids (empty if unknown).
        """
        return list(self._forward.get((source, str(native_id)), []))

    def register(self, source: str, native_id: str, foodatlas_id: str) -> None:
        """Register a primary (source, native_id) → foodatlas_id mapping.

        Used during Pass 1 and Pass 3 entity creation. Raises on duplicate
        keys, which would indicate a bug in the resolution logic.
        """
        key = (source, str(native_id))
        if key in self._forward:
            msg = f"Duplicate registry key: {key} (existing={self._forward[key]})"
            raise ValueError(msg)
        self._forward[key] = [foodatlas_id]
        eid = int(foodatlas_id[len(FAID_PREFIX) :])
        self._max_eid = max(self._max_eid, eid)

    def reassign(self, source: str, native_id: str, foodatlas_id: str) -> str:
        """Re-register an existing key to a new foodatlas_id.

        Used when a seeded mapping points to a stale entity that was not
        rebuilt in the current pipeline run. Returns the old foodatlas_id.
        """
        key = (source, str(native_id))
        old_ids = self._forward.get(key, [])
        old = old_ids[0] if old_ids else ""
        self._forward[key] = [foodatlas_id]
        eid = int(foodatlas_id[len(FAID_PREFIX) :])
        self._max_eid = max(self._max_eid, eid)
        logger.info("Registry reassign: %s from %s → %s.", key, old, foodatlas_id)
        return old

    def register_alias(self, source: str, native_id: str, foodatlas_id: str) -> None:
        """Register a secondary mapping, appending to the list.

        Used during Pass 2 xref linking. Supports 1:N — the same
        (source, native_id) can map to multiple entities.
        """
        key = (source, str(native_id))
        ids = self._forward.setdefault(key, [])
        if foodatlas_id not in ids:
            ids.append(foodatlas_id)

    def all_ids(self) -> set[str]:
        """Return all distinct foodatlas_ids in the registry."""
        return {fid for ids in self._forward.values() for fid in ids}

    def save(self, path: Path | None = None) -> None:
        """Write the registry to parquet."""
        out = path or self._path
        rows = [
            {"source": k[0], "native_id": k[1], "foodatlas_id": fid}
            for k, ids in self._forward.items()
            for fid in ids
        ]
        df = pd.DataFrame(rows, columns=REGISTRY_COLUMNS)
        df.to_parquet(out, index=False)
        logger.info("Registry saved: %d entries to %s.", len(df), out)

    def __len__(self) -> int:
        return len(self._forward)
