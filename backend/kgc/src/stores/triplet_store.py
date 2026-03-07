"""TripletStore — runtime container wrapping a pandas DataFrame."""

import logging
from ast import literal_eval
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

COLUMNS = [
    "foodatlas_id",
    "head_id",
    "relationship_id",
    "tail_id",
    "metadata_ids",
]
FAID_PREFIX = "t"


class TripletStore:
    """Manages relationship triplets in the knowledge graph.

    Each triplet is (head_id, relationship_id, tail_id) with associated
    metadata_ids. A hash table maps composite keys to metadata lists for
    fast deduplication.
    """

    def __init__(self, path_triplets: Path) -> None:
        self.path_triplets = Path(path_triplets)

        self._triplets: pd.DataFrame = pd.DataFrame()
        self._ht_t2m: dict[str, list[str]] = {}
        self._curr_tid: int = 1

        self._load()

    def _load(self) -> None:
        self._triplets = pd.read_csv(
            self.path_triplets,
            sep="\t",
            converters={"metadata_ids": literal_eval},
        ).set_index("foodatlas_id")

        tid = self._triplets.index.str.slice(1).astype(int).max()
        self._curr_tid = tid + 1 if pd.notna(tid) else 1

        self._ht_t2m = {}
        for _, row in self._triplets.iterrows():
            key = f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}"
            self._ht_t2m[key] = row["metadata_ids"]

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        self._triplets.to_csv(path_output_dir / "triplets.tsv", sep="\t")

    def create(self, metadata: pd.DataFrame) -> pd.Series:
        """Create new triplet entries from metadata rows.

        If a triplet already exists, the metadata_id is appended to the
        existing triplet's metadata_ids list (deduplication/merge).
        """
        head_ids = metadata["head_id"].tolist()
        relationship_ids = metadata["relationship_id"].tolist()
        tail_ids = metadata["tail_id"].tolist()
        metadata_ids = metadata.index.tolist()

        rows: list[dict] = []
        for head_id, rel_id, tail_id, meta_id in zip(
            head_ids, relationship_ids, tail_ids, metadata_ids, strict=False
        ):
            key = f"{head_id}_{rel_id}_{tail_id}"
            if key in self._ht_t2m:
                self._ht_t2m[key].append(meta_id)
                continue
            rows.append(
                {
                    "foodatlas_id": f"{FAID_PREFIX}{self._curr_tid}",
                    "head_id": head_id,
                    "relationship_id": rel_id,
                    "tail_id": tail_id,
                    "metadata_ids": None,
                }
            )
            self._curr_tid += 1
            self._ht_t2m[key] = [meta_id]

        if rows:
            triplets_new = pd.DataFrame(rows).set_index("foodatlas_id")
            self._triplets = pd.concat([self._triplets, triplets_new])
        else:
            triplets_new = pd.DataFrame(
                columns=["head_id", "relationship_id", "tail_id", "metadata_ids"]
            )
            triplets_new.index.name = "foodatlas_id"

        def _resolve_metadata(row: pd.Series) -> list[str]:
            key = f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}"
            return list(set(self._ht_t2m[key]))

        self._triplets["metadata_ids"] = self._triplets.apply(_resolve_metadata, axis=1)

        return triplets_new.apply(
            lambda row: self._ht_t2m[
                f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}"
            ],
            axis=1,
        )

    def get_by_relationship_id(self, relationship_id: str) -> pd.DataFrame:
        return self._triplets[
            self._triplets["relationship_id"] == relationship_id
        ].copy()
