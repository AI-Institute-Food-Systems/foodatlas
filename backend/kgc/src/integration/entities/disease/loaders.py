"""Load cleaned CTD data and filter for KG integration."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .constants import CTD_DIRECTEVIDENCE

if TYPE_CHECKING:
    from ....models.settings import KGCSettings
    from ....stores.entity_store import EntityStore


def load_ctd_chemdis(settings: KGCSettings) -> pd.DataFrame:
    """Load cleaned CTD chemical-disease data from parquet."""
    path = Path(settings.data_cleaning_dir) / "ctd_chemdis_cleaned.parquet"
    return pd.read_parquet(path)


def load_ctd_diseases(settings: KGCSettings) -> pd.DataFrame:
    """Load cleaned CTD diseases data from parquet."""
    path = Path(settings.data_cleaning_dir) / "ctd_diseases_cleaned.parquet"
    return pd.read_parquet(path)


def filter_ctd_chemdis(
    ctd_chemdis: pd.DataFrame,
    entity_store: EntityStore,
) -> pd.DataFrame:
    """Filter CTD chemical-disease rows to those with direct evidence and in KG."""
    ctd_chemdis = ctd_chemdis[ctd_chemdis[CTD_DIRECTEVIDENCE].notnull()].reset_index(
        drop=True
    )
    mesh_fa: set[str] = set()
    for _, row in entity_store._entities.iterrows():
        if "mesh" in row["external_ids"]:
            mesh_fa.update(str(m) for m in row["external_ids"]["mesh"])
    ctd_chemdis = ctd_chemdis[ctd_chemdis["ChemicalID"].isin(mesh_fa)].reset_index(
        drop=True
    )
    return ctd_chemdis


def extract_pubmed_ids(ctd_chemdis: pd.DataFrame) -> set[int]:
    """Extract all unique PubMed IDs from filtered CTD data."""
    pubmed_ids: set[int] = set()
    for pids in ctd_chemdis["PubMedIDs"]:
        pubmed_ids.update(int(p) for p in pids if p)
    return pubmed_ids
