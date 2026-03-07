"""TripletStore — runtime container wrapping a pandas DataFrame."""

import logging
from ast import literal_eval
from pathlib import Path

import pandas as pd

from .schema import FILE_TRIPLETS, INDEX_COL, TSV_SEP

logger = logging.getLogger(__name__)

FAID_PREFIX = "t"


class TripletStore:
    """Manages relationship triplets in the knowledge graph.

    Each triplet is (head_id, relationship_id, tail_id) with associated
    metadata_ids. A lookup dict maps composite keys to metadata lists for
    fast deduplication.
    """

    @staticmethod
    def _make_key(head_id: str, rel_id: str, tail_id: str) -> str:
        return f"{head_id}_{rel_id}_{tail_id}"

    def __init__(self, path_triplets: Path) -> None:
        self.path_triplets = Path(path_triplets)

        self._triplets: pd.DataFrame = pd.DataFrame()
        self._key_to_metadata: dict[str, list[str]] = {}
        self._curr_tid: int = 1

        self._load()

    def _load(self) -> None:
        self._triplets = pd.read_csv(
            self.path_triplets,
            sep=TSV_SEP,
            converters={"metadata_ids": literal_eval},
        ).set_index(INDEX_COL)

        max_tid = self._triplets.index.str.slice(1).astype(int).max()
        self._curr_tid = max_tid + 1 if pd.notna(max_tid) else 1

        self._key_to_metadata = {}
        for _, row in self._triplets.iterrows():
            key = self._make_key(row["head_id"], row["relationship_id"], row["tail_id"])
            self._key_to_metadata[key] = row["metadata_ids"]

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        self._triplets.to_csv(path_output_dir / FILE_TRIPLETS, sep=TSV_SEP)

    def create(self, metadata: pd.DataFrame) -> pd.Series:
        """Create new triplet entries from metadata rows.

        If a triplet already exists, the metadata_id is appended to the
        existing triplet's metadata_ids list (deduplication/merge).
        """
        new_rows = self._insert_or_merge(metadata)
        self._resolve_all_metadata()

        return new_rows.apply(
            lambda row: self._key_to_metadata[
                self._make_key(row["head_id"], row["relationship_id"], row["tail_id"])
            ],
            axis=1,
        )

    def _insert_or_merge(self, metadata: pd.DataFrame) -> pd.DataFrame:
        """Insert new triplets or merge metadata into existing ones."""
        rows: list[dict] = []
        for (head_id, rel_id, tail_id), meta_id in zip(
            zip(
                metadata["head_id"],
                metadata["relationship_id"],
                metadata["tail_id"],
                strict=False,
            ),
            metadata.index,
            strict=False,
        ):
            key = self._make_key(head_id, rel_id, tail_id)
            if key in self._key_to_metadata:
                self._key_to_metadata[key].append(meta_id)
                continue
            rows.append(
                {
                    INDEX_COL: f"{FAID_PREFIX}{self._curr_tid}",
                    "head_id": head_id,
                    "relationship_id": rel_id,
                    "tail_id": tail_id,
                    "metadata_ids": None,
                }
            )
            self._curr_tid += 1
            self._key_to_metadata[key] = [meta_id]

        if rows:
            new_triplets = pd.DataFrame(rows).set_index(INDEX_COL)
            self._triplets = pd.concat([self._triplets, new_triplets])
            return new_triplets

        empty = pd.DataFrame(
            columns=["head_id", "relationship_id", "tail_id", "metadata_ids"]
        )
        empty.index.name = INDEX_COL
        return empty

    def _resolve_all_metadata(self) -> None:
        """Sync metadata_ids column with the key_to_metadata lookup."""
        self._triplets["metadata_ids"] = self._triplets.apply(
            lambda row: list(
                set(
                    self._key_to_metadata[
                        self._make_key(
                            row["head_id"], row["relationship_id"], row["tail_id"]
                        )
                    ]
                )
            ),
            axis=1,
        )

    def get_by_relationship_id(self, relationship_id: str) -> pd.DataFrame:
        return self._triplets[
            self._triplets["relationship_id"] == relationship_id
        ].copy()
