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
    """Persistent mapping from (source, native_id) to foodatlas_id.

    Ensures that the same real-world entity always receives the same
    foodatlas_id across pipeline rebuilds.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._forward: dict[tuple[str, str], str] = {}
        self._max_eid: int = 0
        self._load()

    def _load(self) -> None:
        if self._path.exists() and self._path.stat().st_size > 0:
            df = pd.read_parquet(self._path)
            for _, row in df.iterrows():
                key = (str(row["source"]), str(row["native_id"]))
                self._forward[key] = str(row["foodatlas_id"])
            self._recompute_max_eid()
        logger.info(
            "Registry loaded: %d entries, max eid=%d.", len(self), self._max_eid
        )

    def _recompute_max_eid(self) -> None:
        if not self._forward:
            self._max_eid = 0
            return
        ids = self._forward.values()
        self._max_eid = max(
            int(fid[len(FAID_PREFIX) :]) for fid in ids if fid.startswith(FAID_PREFIX)
        )

    @property
    def next_eid(self) -> int:
        """Next available entity ID number (e.g. 217 means next ID is 'e217')."""
        return self._max_eid + 1

    def resolve(self, source: str, native_id: str) -> str:
        """Look up foodatlas_id for *(source, native_id)*.

        Returns the foodatlas_id, or ``""`` if the pair is unknown.
        """
        return self._forward.get((source, str(native_id)), "")

    def register(self, source: str, native_id: str, foodatlas_id: str) -> None:
        """Register a primary (source, native_id) → foodatlas_id mapping.

        Used during Pass 1 and Pass 3 entity creation. Raises on duplicate
        keys, which would indicate a bug in the resolution logic.
        """
        key = (source, str(native_id))
        if key in self._forward:
            msg = f"Duplicate registry key: {key} (existing={self._forward[key]})"
            raise ValueError(msg)
        self._forward[key] = foodatlas_id
        eid = int(foodatlas_id[len(FAID_PREFIX) :])
        self._max_eid = max(self._max_eid, eid)

    def register_alias(self, source: str, native_id: str, foodatlas_id: str) -> str:
        """Register a secondary (source, native_id) mapping for an existing entity.

        Used during Pass 2 xref linking.

        Returns:
            ``""`` if the key was new or already pointed to the same entity.
            The *old* foodatlas_id if the key existed with a different entity
            (indicates a merge — the caller should track this).
        """
        key = (source, str(native_id))
        existing = self._forward.get(key, "")
        if existing == foodatlas_id:
            return ""
        if existing:
            logger.info(
                "Registry re-link: %s moved %s → %s.", key, existing, foodatlas_id
            )
            self._forward[key] = foodatlas_id
            return existing
        self._forward[key] = foodatlas_id
        return ""

    def all_ids(self) -> set[str]:
        """Return all distinct foodatlas_ids in the registry."""
        return set(self._forward.values())

    def save(self, path: Path | None = None) -> None:
        """Write the registry to parquet."""
        out = path or self._path
        rows = [
            {"source": k[0], "native_id": k[1], "foodatlas_id": v}
            for k, v in self._forward.items()
        ]
        df = pd.DataFrame(rows, columns=REGISTRY_COLUMNS)
        df.to_parquet(out, index=False)
        logger.info("Registry saved: %d entries to %s.", len(df), out)

    def __len__(self) -> int:
        return len(self._forward)
