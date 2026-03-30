"""Stage 1: Apply centralized corrections to Phase 1 ingest output."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..config.corrections import Corrections

logger = logging.getLogger(__name__)


def apply_corrections(
    sources: dict[str, dict[str, pd.DataFrame]],
    corrections: Corrections,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Mutate Phase 1 DataFrames in-place according to corrections.yaml.

    Args:
        sources: ``{source_id: {"nodes": df, "edges": df, "xrefs": df}}``.
        corrections: Loaded ``Corrections`` object.

    Returns:
        The same dict (mutated in-place for convenience).
    """
    _apply_chebi_corrections(sources, corrections)
    _apply_cdno_corrections(sources, corrections)
    _apply_fdc_corrections(sources, corrections)
    return sources


def _apply_chebi_corrections(
    sources: dict[str, dict[str, pd.DataFrame]],
    corrections: Corrections,
) -> None:
    chebi = sources.get("chebi")
    if chebi is None:
        return
    nodes: pd.DataFrame = chebi["nodes"]

    if corrections.chebi.drop_nodes:
        drop_ids = set(corrections.chebi.drop_nodes)
        before = len(nodes)
        nodes = nodes[~nodes["native_id"].isin(drop_ids)].copy()
        chebi["nodes"] = nodes
        logger.info("ChEBI: dropped %d nodes.", before - len(nodes))

    for native_id, renames in corrections.chebi.rename_nodes.items():
        mask = nodes["native_id"] == native_id
        if mask.any():
            for field, value in renames.items():
                nodes.loc[mask, field] = value
            logger.info("ChEBI: renamed node %s.", native_id)


def _apply_cdno_corrections(
    sources: dict[str, dict[str, pd.DataFrame]],
    corrections: Corrections,
) -> None:
    cdno = sources.get("cdno")
    if cdno is None:
        return
    xrefs: pd.DataFrame = cdno.get("xrefs", pd.DataFrame())

    for remap in corrections.cdno.remap_xrefs:
        mask = (xrefs["target_source"] == remap.target_source) & (
            xrefs["target_id"] == remap.old_target
        )
        if mask.any():
            xrefs.loc[mask, "target_id"] = remap.new_target
            logger.info(
                "CDNO: remapped xref %s → %s.", remap.old_target, remap.new_target
            )

    cdno["xrefs"] = xrefs


def _apply_fdc_corrections(
    sources: dict[str, dict[str, pd.DataFrame]],
    corrections: Corrections,
) -> None:
    fdc = sources.get("fdc")
    if fdc is None:
        return
    nodes: pd.DataFrame = fdc["nodes"]

    drop_ids = {
        f"nutrient:{did}" for did in corrections.fdc.nutrient_overrides.drop_ids
    }
    if drop_ids:
        before = len(nodes)
        nodes = nodes[~nodes["native_id"].isin(drop_ids)].copy()
        fdc["nodes"] = nodes
        logger.info("FDC: dropped %d nutrient nodes.", before - len(nodes))

    for nutrient_id, new_name in corrections.fdc.nutrient_overrides.renames.items():
        mask = nodes["native_id"] == f"nutrient:{nutrient_id}"
        if mask.any():
            nodes.loc[mask, "name"] = new_name
            renamed = new_name
            nodes.loc[mask, "synonyms"] = nodes.loc[mask, "synonyms"].apply(
                lambda _, n=renamed: [n]
            )
