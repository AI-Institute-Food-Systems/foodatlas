"""Promote the per-measurement bioactivity table into the KG output.

The ingest stage writes the full raw ``bioactivity_measurements.parquet`` (~3.7M
rows). Here we prune it to the ``bm…`` ids actually referenced by surviving
bioactivity triplets and write it to ``kg_dir`` as
``attestations_bioactivity.parquet`` — the bioactivity attestation store (a
typed, domain-specific sibling of ``attestations.parquet``). The DB loader finds
it in ``kg_dir`` and joins it from ``r5``/``r6`` triplets' ``attestation_ids``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ....models.relationship import RelationshipType

if TYPE_CHECKING:
    from ....models.settings import KGCSettings
    from ...knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_INGEST_FILE = "bioactivity_measurements.parquet"  # raw passthrough from ingest
_KG_FILE = "attestations_bioactivity.parquet"  # the bioactivity attestation store
_ID_COL = "bioactivity_metadata_id"
_ASSOC_RELS = {str(RelationshipType.EXHIBITS), str(RelationshipType.MEASURED)}


def promote_bioactivity_measurements(
    settings: KGCSettings, kg: KnowledgeGraph
) -> None:
    """Prune ingested measurements to referenced ids and write to kg_dir."""
    src = Path(settings.ingest_dir) / "bioactivity" / _INGEST_FILE
    if not src.exists():
        logger.info("No %s to promote (skipping).", _INGEST_FILE)
        return

    referenced = _referenced_measurement_ids(kg)
    measurements = pd.read_parquet(src)
    pruned = measurements[measurements[_ID_COL].isin(referenced)]

    out = Path(settings.kg_dir) / _KG_FILE
    pruned.to_parquet(out, index=False)
    logger.info(
        "Promoted %d/%d bioactivity measurements to %s.",
        len(pruned),
        len(measurements),
        out,
    )


def _referenced_measurement_ids(kg: KnowledgeGraph) -> set[str]:
    triplets = kg.triplets._triplets
    assoc = triplets[triplets["relationship_id"].isin(_ASSOC_RELS)]
    referenced: set[str] = set()
    for ids in assoc["attestation_ids"]:
        referenced.update(ids)
    return referenced
