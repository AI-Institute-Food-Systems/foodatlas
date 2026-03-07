"""MetadataContainsStore — runtime container wrapping a pandas DataFrame."""

import logging
from ast import literal_eval
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

COLUMNS = [
    "foodatlas_id",
    "conc_value",
    "conc_unit",
    "food_part",
    "food_processing",
    "source",
    "reference",
    "entity_linking_method",
    "quality_score",
    "_food_name",
    "_chemical_name",
    "_conc",
    "_food_part",
]
FAID_PREFIX = "mc"


class MetadataContainsStore:
    """Manages metadata records for "contains" relationship triplets.

    Stores concentration, food parts, processing, sources, and quality
    scores with auto-generated IDs (prefix "mc").
    """

    def __init__(self, path_metadata_contains: Path) -> None:
        self.path_metadata_contains = Path(path_metadata_contains)

        self._metadata_contains: pd.DataFrame = pd.DataFrame()
        self._curr_mcid: int = 1

        self._load()

    def _load(self) -> None:
        self._metadata_contains = pd.read_csv(
            self.path_metadata_contains,
            sep="\t",
            converters={
                "reference": literal_eval,
                "_conc": lambda x: "" if pd.isna(x) else x,
                "_food_part": lambda x: "" if pd.isna(x) else x,
            },
        ).set_index("foodatlas_id")

        mcid = self._metadata_contains.index.str.slice(2).astype(int).max()
        self._curr_mcid = mcid + 1 if pd.notna(mcid) else 1

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        self._metadata_contains.to_csv(
            path_output_dir / "metadata_contains.tsv", sep="\t"
        )

    def create(self, metadata: pd.DataFrame) -> pd.DataFrame:
        """Add new metadata entries with auto-generated IDs."""
        metadata = metadata.reset_index(drop=True)
        metadata["foodatlas_id"] = FAID_PREFIX + (
            self._curr_mcid + metadata.index
        ).astype(str)
        metadata = metadata[COLUMNS].set_index("foodatlas_id")

        self._curr_mcid += len(metadata)
        self._metadata_contains = pd.concat(
            [self._metadata_contains, metadata],
        )
        return metadata

    def get(self, metadata_ids: list[str]) -> pd.DataFrame:
        return self._metadata_contains.loc[metadata_ids]
