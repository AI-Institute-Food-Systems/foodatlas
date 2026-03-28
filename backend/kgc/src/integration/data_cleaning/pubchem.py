"""Process PubChem SID-Map to extract ChEBI entries."""

import logging
from pathlib import Path

import pandas as pd

from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def process_pubchem(settings: KGCSettings) -> None:
    """Filter PubChem SID-Map for ChEBI entries and save as parquet."""
    data_dir = Path(settings.data_dir)
    dp_dir = Path(settings.data_cleaning_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    sids: pd.DataFrame = pd.read_csv(
        data_dir / "PubChem" / "SID-Map",
        sep="\t",
        header=None,
        names=["SID", "source", "registry_id", "cid"],
    )

    sids.query("source == 'ChEBI'").to_parquet(dp_dir / "pubchem-sid-map-small.parquet")
    logger.info("Processed PubChem SID-Map.")
