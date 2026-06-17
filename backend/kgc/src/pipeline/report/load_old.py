"""Load previous KG snapshot (parquet) for changelog comparison."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from ...utils.snapshots import latest_snapshot

logger = logging.getLogger(__name__)


@dataclass
class OldKG:
    """Normalized old KG data ready for comparison."""

    entities: pd.DataFrame
    triplets: pd.DataFrame
    metadata_contains_sources: pd.Series
    metadata_diseases_sources: pd.Series


def load_old_kg(data_dir: str) -> OldKG:
    """Load the latest KG snapshot from data_dir/PreviousFAKG/<latest>/."""
    snapshot = latest_snapshot(data_dir)
    if snapshot is None:
        raise FileNotFoundError(f"No snapshots found under {data_dir}/PreviousFAKG")
    logger.info("Loading previous KG snapshot from %s", snapshot)

    entities = pd.read_parquet(snapshot / "entities.parquet")
    if "foodatlas_id" in entities.columns:
        entities = entities.set_index("foodatlas_id")

    triplets = pd.read_parquet(
        snapshot / "triplets.parquet",
        columns=["head_id", "relationship_id", "tail_id"],
    )

    attestations = pd.read_parquet(
        snapshot / "attestations.parquet", columns=["source"]
    )
    # Split attestations into contains (non-CTD) and diseases (CTD) to
    # approximate the old metadata_contains / metadata_diseases TSV split.
    attestations["source"] = attestations["source"].astype(str)
    is_ctd = attestations["source"].str.startswith("ctd")
    contains_sources = attestations[~is_ctd]["source"].value_counts()
    diseases_sources = attestations[is_ctd]["source"].value_counts()

    logger.info(
        "Loaded old KG snapshot: %d entities, %d triplets.",
        len(entities),
        len(triplets),
    )
    return OldKG(
        entities=entities,
        triplets=triplets,
        metadata_contains_sources=contains_sources,
        metadata_diseases_sources=diseases_sources,
    )
