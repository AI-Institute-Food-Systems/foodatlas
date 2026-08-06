"""Promote the bioactivity disease↔assay bridge to kg_dir.

The ingest stage writes two passthrough files:
* ``bioactivity_disease.parquet`` — disease (MeSH) → assay (``source_assay_id``)
  links, with the CTD ``relationship`` type(s) and the ``bdm…`` metadata ids.
* ``bioactivity_disease_targets.parquet`` — each ``bdm…`` id → its target gene ids.

Here we copy both into ``kg_dir`` so the DB loads them as
``base_bioactivity_disease`` / ``base_bioactivity_disease_targets``. The
materializer then joins the disease→assay bridge to chemical measurements
(chemical *active* in the same assay) to infer chemical↔disease associations.
No entity ids are minted; diseases resolve to KG entities by MeSH at DB time.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)

_FILES = ("bioactivity_disease.parquet", "bioactivity_disease_targets.parquet")


def promote_bioactivity_disease(settings: KGCSettings) -> None:
    """Copy the disease-bridge passthroughs from the ingest dir to kg_dir."""
    src_dir = Path(settings.ingest_dir) / "bioactivity"
    if not (src_dir / _FILES[0]).exists():
        logger.info("No bioactivity-disease bridge to promote (skipping).")
        return

    kg_dir = Path(settings.kg_dir)
    for name in _FILES:
        df = pd.read_parquet(src_dir / name)
        df.to_parquet(kg_dir / name, index=False)
        logger.info("Promoted %d rows to %s.", len(df), kg_dir / name)
