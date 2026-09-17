"""Promote the food-chemical-bioactivity efficacy table to kg_dir.

The ingest stage writes the raw passthrough
``bioactivity_food_chemical_efficacy.parquet`` (one row per
foodxchemicalxbioactivity, ~61k). Here we copy it to ``kg_dir`` as
``food_chemical_efficacy.parquet`` — the fact table the DB loads as
``base_food_chemical_efficacy`` and resolves (``cid`` → chemical entity,
``E300…`` → bioactivity concept) into ``mv_food_chemical_efficacy``.

Food, chemical, and bioactivity are all *existing* KG entities (foods by
``foodatlas_id``, chemicals by PubChem ``cid``, concepts by native id), so no
ids are minted here and there is nothing to prune — resolution and any dropping
of unresolved rows happen once, in the DB materializer.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)

_INGEST_FILE = "bioactivity_food_chemical_efficacy.parquet"  # raw passthrough
_KG_FILE = "food_chemical_efficacy.parquet"  # the fact table the DB loads


def promote_food_chemical_efficacy(settings: KGCSettings) -> None:
    """Copy the efficacy passthrough from the ingest dir to kg_dir."""
    src = Path(settings.ingest_dir) / "bioactivity" / _INGEST_FILE
    if not src.exists():
        logger.info("No food-chemical efficacy to promote (skipping).")
        return

    efficacy = pd.read_parquet(src)
    out = Path(settings.kg_dir) / _KG_FILE
    efficacy.to_parquet(out, index=False)
    logger.info("Promoted %d food-chemical efficacy rows to %s.", len(efficacy), out)
