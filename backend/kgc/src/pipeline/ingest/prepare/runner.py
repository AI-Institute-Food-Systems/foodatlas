"""One delivery → both source directories, validated before anything is written."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from .bioactivity import prepare_bioactivity, write_bioactivity
from .ptfi import load_delta, prepare_ptfi
from .snapshot import load_snapshot_entities

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

    from .ptfi import PtfiSource

logger = logging.getLogger(__name__)


def prepare_delivery(delivery_dir: Path, snapshot_dir: Path, data_dir: Path) -> None:
    """Rewrite *delivery_dir* into ``<data_dir>/PTFI`` and ``…/Bioactivity``."""
    logger.info("Preparing %s against snapshot %s", delivery_dir, snapshot_dir)
    delta = load_delta(delivery_dir / "ptfi")
    snapshot = load_snapshot_entities(snapshot_dir)

    ptfi = prepare_ptfi(delta, snapshot)
    food_edges = prepare_bioactivity(delivery_dir, delta["entities"], snapshot)

    ptfi.write(data_dir / "PTFI")
    write_bioactivity(delivery_dir, food_edges, data_dir / "Bioactivity")
    report(ptfi, food_edges, data_dir)


def report(ptfi: PtfiSource, food_edges: pd.DataFrame, data_dir: Path) -> None:
    """Summarize what was written, flagging entities no measurement touches —
    PTFI catalogues far more chemicals than it detects, and the adapter skips
    the unmeasured ones."""
    measured_samples = set(ptfi.measurements["sample_id"])
    measured_keys = set(ptfi.measurements["chemical_key"])
    foods_measured = sum(
        1
        for ids in ptfi.foods["ptfi_sample_ids"]
        if measured_samples.intersection(json.loads(ids))
    )
    logger.info("PTFI source written to %s:", data_dir / "PTFI")
    logger.info(
        "  foods        %5d  (%d with a FoodOn IRI; %d measured)",
        len(ptfi.foods),
        int((ptfi.foods["foodon_iri"] != "").sum()),
        foods_measured,
    )
    logger.info(
        "  chemicals    %5d  (%d PTFI-described, %d pre-existing; %d measured)",
        len(ptfi.chemicals),
        int((ptfi.chemicals["ptfi_id"] != "").sum()),
        int((ptfi.chemicals["ptfi_id"] == "").sum()),
        int(ptfi.chemicals["chemical_key"].isin(measured_keys).sum()),
    )
    logger.info(
        "  measurements %5d  over %d samples x %d chemicals, %d evidence records",
        len(ptfi.measurements),
        len(measured_samples),
        len(measured_keys),
        len(ptfi.evidence),
    )
    by_unit = ptfi.measurements["conc_unit"].value_counts().to_dict()
    logger.info("  by unit      %s", by_unit)
    logger.info(
        "Bioactivity source written to %s: %d whole-food rows on %d foods.",
        data_dir / "Bioactivity",
        len(food_edges),
        food_edges[["foodatlas_id", "ptfi_sample_id"]]
        .astype(str)
        .sum(axis=1)
        .nunique(),
    )
