"""Load old v3.3 KG TSV files into normalized DataFrames."""

from __future__ import annotations

import ast
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class OldKG:
    """Normalized old KG data ready for comparison."""

    entities: pd.DataFrame
    triplets: pd.DataFrame
    metadata_contains_sources: pd.Series
    metadata_diseases_sources: pd.Series


def _safe_literal(val: Any) -> Any:
    """Parse a Python literal string, returning None on failure."""
    try:
        return ast.literal_eval(str(val))
    except (ValueError, SyntaxError):
        return None


def load_old_entities(path: Path) -> pd.DataFrame:
    """Load ``entities.tsv`` and parse list/dict columns."""
    df = pd.read_csv(path, sep="\t")
    df["synonyms"] = df["synonyms"].apply(_safe_literal)
    df["external_ids"] = df["external_ids"].apply(_safe_literal)
    return df.set_index("foodatlas_id")


def load_old_triplets(path_triplets: Path, path_ontology: Path) -> pd.DataFrame:
    """Load ``triplets.tsv`` + ``food_ontology.tsv`` and combine."""
    cols = ["head_id", "relationship_id", "tail_id"]
    t = pd.read_csv(path_triplets, sep="\t", usecols=cols)
    o = pd.read_csv(path_ontology, sep="\t", usecols=cols)
    return pd.concat([t, o], ignore_index=True)


def load_old_kg(data_dir: str) -> OldKG:
    """Load all old v3.3 KG files from *data_dir*/PreviousFAKG/v3.3."""
    base = Path(data_dir) / "PreviousFAKG" / "v3.3"
    entities = load_old_entities(base / "entities.tsv")
    triplets = load_old_triplets(
        base / "triplets.tsv",
        base / "food_ontology.tsv",
    )
    mc = pd.read_csv(
        base / "metadata_contains.tsv",
        sep="\t",
        usecols=["source"],
        low_memory=False,
    )
    md = pd.read_csv(
        base / "metadata_diseases.tsv",
        sep="\t",
        usecols=["source"],
    )
    logger.info(
        "Loaded old KG: %d entities, %d triplets.",
        len(entities),
        len(triplets),
    )
    return OldKG(
        entities=entities,
        triplets=triplets,
        metadata_contains_sources=mc["source"].value_counts(),
        metadata_diseases_sources=md["source"].value_counts(),
    )
