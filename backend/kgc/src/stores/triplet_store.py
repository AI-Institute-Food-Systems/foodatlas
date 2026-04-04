"""TripletStore — runtime container wrapping a pandas DataFrame."""

import json
import logging
from pathlib import Path

import pandas as pd

from .schema import FILE_TRIPLETS

logger = logging.getLogger(__name__)

_KEY_COL = "_key"


class TripletStore:
    """Manages relationship triplets in the knowledge graph.

    Each triplet is identified by its composite key
    ``(head_id, relationship_id, tail_id)`` — no separate ID column.
    A lookup dict maps composite keys to metadata lists for deduplication.
    """

    @staticmethod
    def _make_key(head_id: str, rel_id: str, tail_id: str) -> str:
        return f"{head_id}_{rel_id}_{tail_id}"

    def __init__(self, path_triplets: Path) -> None:
        self.path_triplets = Path(path_triplets)

        self._triplets: pd.DataFrame = pd.DataFrame()
        self._key_to_extractions: dict[str, list[str]] = {}

        self._load()

    def _load(self) -> None:
        if self.path_triplets.exists() and self.path_triplets.stat().st_size > 0:
            self._triplets = pd.read_parquet(self.path_triplets)
            if self._triplets.empty or "head_id" not in self._triplets.columns:
                self._triplets = pd.DataFrame()
                return
            if "extraction_ids" in self._triplets.columns:
                self._triplets["extraction_ids"] = self._triplets[
                    "extraction_ids"
                ].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
            self._triplets[_KEY_COL] = self._triplets.apply(
                lambda r: self._make_key(
                    r["head_id"], r["relationship_id"], r["tail_id"]
                ),
                axis=1,
            )
            self._triplets = self._triplets.set_index(_KEY_COL)
        else:
            self._triplets = pd.DataFrame()

        self._key_to_extractions = {}
        for key, row in self._triplets.iterrows():
            self._key_to_extractions[key] = row.get("extraction_ids", []) or []

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        df = self._triplets.reset_index(drop=True)
        if "extraction_ids" in df.columns:
            df["extraction_ids"] = df["extraction_ids"].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else x
            )
        df.to_parquet(path_output_dir / FILE_TRIPLETS, index=False)

    def add_ontology(self, triplets: pd.DataFrame) -> None:
        """Add pre-built ontology triplets (no extraction_ids)."""
        if triplets.empty:
            return
        if "extraction_ids" not in triplets.columns:
            triplets["extraction_ids"] = None
        # Drop any legacy ID column.
        if "foodatlas_id" in triplets.columns:
            triplets = triplets.drop(columns=["foodatlas_id"])
        triplets[_KEY_COL] = triplets.apply(
            lambda r: self._make_key(r["head_id"], r["relationship_id"], r["tail_id"]),
            axis=1,
        )
        triplets = triplets.set_index(_KEY_COL)
        for key in triplets.index:
            self._key_to_extractions[key] = []
        self._triplets = pd.concat([self._triplets, triplets])

    def create(self, metadata: pd.DataFrame) -> pd.Series:
        """Create new triplet entries from metadata rows.

        If a triplet already exists, the metadata_id is appended to the
        existing triplet's extraction_ids list (deduplication/merge).
        """
        new_rows = self._insert_or_merge(metadata)
        self._resolve_all_metadata()

        return new_rows.apply(
            lambda row: self._key_to_extractions[
                self._make_key(row["head_id"], row["relationship_id"], row["tail_id"])
            ],
            axis=1,
        )

    def _insert_or_merge(self, metadata: pd.DataFrame) -> pd.DataFrame:
        """Insert new triplets or merge metadata into existing ones."""
        if "source" not in metadata.columns:
            metadata = metadata.assign(source="")

        # Build keys vectorized.
        keys = (
            metadata["head_id"]
            + "_"
            + metadata["relationship_id"]
            + "_"
            + metadata["tail_id"]
        )
        meta_ids = metadata.index

        # Split into existing (merge) and new (insert).
        existing_mask = keys.isin(self._key_to_extractions)

        # Merge extraction IDs into existing triplets.
        for key, meta_id in zip(
            keys[existing_mask], meta_ids[existing_mask], strict=False
        ):
            self._key_to_extractions[key].append(meta_id)

        # Bulk insert new triplets.
        new_mask = ~existing_mask
        if new_mask.any():
            new_df = metadata.loc[
                new_mask, ["head_id", "relationship_id", "tail_id", "source"]
            ].copy()
            new_df[_KEY_COL] = keys[new_mask].values
            new_df["extraction_ids"] = None
            new_df = new_df.set_index(_KEY_COL)
            self._triplets = pd.concat([self._triplets, new_df])

            for key, meta_id in zip(keys[new_mask], meta_ids[new_mask], strict=False):
                self._key_to_extractions[key] = [meta_id]

        return metadata

    def _resolve_all_metadata(self) -> None:
        """Sync extraction_ids column with the key_to_metadata lookup."""
        self._triplets["extraction_ids"] = [
            list(set(self._key_to_extractions.get(key, [])))
            for key in self._triplets.index
        ]

    def filter(
        self,
        head_id: str | None = None,
        tail_id: str | None = None,
    ) -> pd.DataFrame:
        """Return triplets matching optional head/tail filters."""
        result = self._triplets
        if head_id is not None:
            result = result[result["head_id"] == head_id]
        if tail_id is not None:
            result = result[result["tail_id"] == tail_id]
        return result.copy()

    def get_by_relationship_id(self, relationship_id: str) -> pd.DataFrame:
        return self._triplets[
            self._triplets["relationship_id"] == relationship_id
        ].copy()
