"""Readers for the prepared PTFI source under ``data/PTFI/``."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

FOOD_PREFIX = "PTFI_FOOD"  # native id namespace for a food group
CHEM_PREFIX = "PTFI_CHEM"  # native id namespace for a chemical (by its key)


def read_foods(path: Path) -> pd.DataFrame:
    """PTFI food groups; ``ptfi_sample_ids`` is the list of GGB ids in each."""
    foods = pd.read_csv(path).fillna("")
    for column in ("ptfi_sample_ids", "synonyms", "food_groups"):
        foods[column] = foods[column].map(_parse_list)
    # The group's native id is its lowest sample id: stable, and every sample id
    # is registered as an alias, so resolution survives the set changing.
    foods["native_id"] = foods["ptfi_sample_ids"].map(
        lambda ids: f"{FOOD_PREFIX}:{min(ids)}"
    )
    return foods


def read_chemicals(path: Path) -> pd.DataFrame:
    """Chemicals PTFI measured, one row per ``chemical_key``."""
    chemicals = pd.read_csv(path, dtype=str).fillna("")
    chemicals["native_id"] = CHEM_PREFIX + ":" + chemicals["chemical_key"]
    return chemicals


def read_measurements(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"chemical_key": str, "evidence_id": str}).fillna(
        {"conc_unit": ""}
    )


def read_evidence(path: Path) -> dict[str, dict]:
    """Evidence id → the reference dict the triplets stage stores verbatim."""
    evidence = pd.read_csv(path, dtype=str)
    return {
        evidence_id: json.loads(reference)
        for evidence_id, reference in zip(
            evidence["evidence_id"], evidence["reference"], strict=False
        )
    }


def _parse_list(cell: object) -> list:
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str) and cell.strip():
        try:
            value = json.loads(cell)
        except json.JSONDecodeError:
            return []
        return value if isinstance(value, list) else []
    return []
