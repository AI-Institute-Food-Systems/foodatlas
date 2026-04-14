"""Ambiguity tracking — filter ambiguous attestations to a separate parquet."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ...stores.schema import FILE_ATTESTATIONS_AMBIGUOUS

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

    from ...stores.attestation_store import AttestationStore

logger = logging.getLogger(__name__)


def write_ambiguous_attestations(store: AttestationStore, output_dir: Path) -> None:
    """Write attestations with multi-valued candidates to a separate parquet."""
    if store._records.empty:
        return
    if "head_candidates" not in store._records.columns:
        return

    def _is_ambiguous(row: pd.Series) -> bool:
        hc = row.get("head_candidates", [])
        tc = row.get("tail_candidates", [])
        return (hasattr(hc, "__len__") and len(hc) > 1) or (
            hasattr(tc, "__len__") and len(tc) > 1
        )

    mask = store._records.apply(_is_ambiguous, axis=1)
    ambiguous = store._records[mask]

    if ambiguous.empty:
        logger.info("No ambiguous attestations found.")
        return

    out = output_dir / FILE_ATTESTATIONS_AMBIGUOUS
    ambiguous.reset_index().to_parquet(out, index=False)
    logger.info(
        "Wrote %d ambiguous attestations to %s.",
        len(ambiguous),
        out,
    )
