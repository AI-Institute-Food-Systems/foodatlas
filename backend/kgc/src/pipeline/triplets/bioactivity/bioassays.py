"""Prune the assay dimension to assays referenced by surviving measurements.

The ingest stage writes the full ``bioactivity_bioassays.parquet`` (~448k
assays). Here we keep only the assays referenced by the pruned measurement store
(``attestations_bioactivity.parquet``) and write ``bioassays.parquet`` to
``kg_dir`` — the assay dimension the DB loads as ``base_bioassays`` and the
materializer joins onto each displayed measurement by ``source_assay_id``.

Runs after :func:`promote_bioactivity_measurements`, which produces the pruned
measurement store this step reads.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)

_INGEST_FILE = "bioactivity_bioassays.parquet"  # raw passthrough from ingest
_MEASUREMENTS_FILE = "attestations_bioactivity.parquet"  # pruned measurement store
_KG_FILE = "bioassays.parquet"  # the assay dimension
_ASSAY_COL = "source_assay_id"


def promote_bioassays(settings: KGCSettings) -> None:
    """Prune assay metadata to referenced assays and write to kg_dir."""
    src = Path(settings.ingest_dir) / "bioactivity" / _INGEST_FILE
    measurements = Path(settings.kg_dir) / _MEASUREMENTS_FILE
    if not src.exists() or not measurements.exists():
        logger.info("No bioassays to promote (skipping).")
        return

    referenced = set(
        pd.read_parquet(measurements, columns=[_ASSAY_COL])[_ASSAY_COL].dropna()
    )
    bioassays = pd.read_parquet(src)
    pruned = bioassays[bioassays[_ASSAY_COL].isin(referenced)]

    out = Path(settings.kg_dir) / _KG_FILE
    pruned.to_parquet(out, index=False)
    logger.info("Promoted %d/%d bioassays to %s.", len(pruned), len(bioassays), out)
