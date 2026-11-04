"""Bioactivity passthrough tables — read by downstream stages by path, not
resolved into nodes or edges (see the adapter module docstring)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

# Verbose source columns → canonical measurement schema (see KG plan).
_MEASUREMENT_RENAME = {
    "evidence_value_potency_value": "potency_value",
    "evidence_value_potency_unit": "potency_unit",
    "evidence_value_efficacy_zeroactivity": "efficacy_zeroactivity",
    "evidence_value_efficacy_infiniteactivity": "efficacy_infiniteactivity",
    "evidence_value_efficacy_logac50_value": "efficacy_logac50_value",
    "evidence_value_efficacy_hillslope": "efficacy_hillslope",
}

# Efficacy source columns → clean lowercase snake_case (mixed-case cols become
# case-sensitive quoted identifiers in Postgres COPY; normalize them up front).
_EFFICACY_RENAME = {
    "CID": "cid",
    "food_conc_M": "food_conc_m",
    "food_conc_logM": "food_conc_logm",
}


def build_measurements(path: Path) -> pd.DataFrame:
    """Typed passthrough of the per-measurement assay table (~3.7M rows)."""
    df = pd.read_csv(path, low_memory=False)
    return df.rename(columns=_MEASUREMENT_RENAME)


def build_bioassays(path: Path) -> pd.DataFrame:
    """Assay-level metadata passthrough (one row per source_assay_id).

    The assay dimension joined onto each measurement by source_assay_id; carries
    description + target (name/organism/UniProt/Entrez) moved out of the
    measurement table.
    """
    df = pd.read_csv(path, low_memory=False)
    df["bioactivity_ids"] = df["bioactivity_ids"].apply(parse_json_list)
    return df


def build_food_chemical_efficacy(path: Path) -> pd.DataFrame:
    """Typed passthrough of the foodxchemicalxbioactivity efficacy table.

    One row per (``foodatlas_id``, ``cid``, ``bioactivity_id``); ``bioactivity_id``
    is kept verbatim (an ``E300…`` concept or the literal ``"UNCLASSIFIED"``).
    Numeric NaNs are preserved (they become COPY NULLs at load time).
    """
    df = pd.read_csv(path).rename(columns=_EFFICACY_RENAME)
    df["saturated"] = df["saturated"].astype(bool)
    return df


def build_disease(path: Path) -> pd.DataFrame:
    """Disease↔assay bridge passthrough (Phase-2 input)."""
    df = pd.read_csv(path)
    df["relationship"] = df["relationship"].apply(parse_json_list)
    df["bioactivity_disease_metadata_id"] = df["bioactivity_disease_metadata_id"].apply(
        parse_json_list
    )
    return df


def build_disease_targets(path: Path) -> pd.DataFrame:
    """Disease-metadata → bridging target ids passthrough (Phase-2 input)."""
    df = pd.read_csv(path)
    df["target_ids"] = df["target_ids"].apply(parse_json_list)
    return df


def parse_json_list(cell: object) -> list[str]:
    """Parse a JSON-array CSV cell (e.g. Synonyms, bioactivity_metadata_ids)."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    return list(json.loads(cell))
