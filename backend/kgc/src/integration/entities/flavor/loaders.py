"""Load cleaned flavor metadata and filter for KG integration."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ....models.settings import KGCSettings


def load_flavor_metadata(settings: KGCSettings) -> pd.DataFrame:
    """Load cleaned flavor metadata from parquet."""
    path = Path(settings.data_cleaning_dir) / "flavor_metadata_cleaned.parquet"
    return pd.read_parquet(path)


def filter_flavor_metadata(
    metadata: pd.DataFrame,
    entities_df: pd.DataFrame,
) -> pd.DataFrame:
    """Filter flavor metadata to PubChem IDs present in entity store."""
    if metadata.empty:
        return metadata

    chemicals = entities_df[entities_df["entity_type"] == "chemical"].copy()
    chemicals["pc_id"] = chemicals["external_ids"].apply(
        lambda x: int(x["pubchem_compound"][0]) if "pubchem_compound" in x else None
    )
    chemicals = chemicals.dropna(subset=["pc_id"])
    chemicals["pc_id"] = chemicals["pc_id"].astype(int)

    entity_pc_ids = set(chemicals["pc_id"])
    pc_id_to_name = dict(
        zip(chemicals["pc_id"], chemicals["common_name"], strict=False)
    )

    filtered = metadata[metadata["_pubchem_id"].isin(entity_pc_ids)].copy()
    filtered["_chemical"] = filtered["_pubchem_id"].map(pc_id_to_name)
    return filtered.reset_index(drop=True)
