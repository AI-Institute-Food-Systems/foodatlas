"""Build mv_disease_bioactivity — disease↔bioactivity↔chemical, assay-attributed.

A row means: this chemical has ≥1 *Active* measurement in an assay that both
(a) the bioactivity-disease bridge ties to this disease, and (b) is classified
under this bioactivity.

Condition (b) is what separates this view from
``mv_chemical_disease_bioactivity``. Reaching a bioactivity by way of
disease → chemical → every bioactivity that chemical was ever measured for
attributes activities to a disease that nothing in its assays supports — the
resulting list ranks by how many chemicals a disease has, not by what it does.
Attributing through the bridging assay keeps only the activities that carried
the disease link in the first place.
"""

import logging

import pandas as pd
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy
from .materializer_bioactivity_bridge import (
    assay_bioactivity_map,
    build_bridge_evidence,
)

logger = logging.getLogger(__name__)

_MV_COLUMNS = [
    "disease_name",
    "disease_foodatlas_id",
    "bioactivity_name",
    "bioactivity_foodatlas_id",
    "chemical_name",
    "chemical_foodatlas_id",
    "n_assays",
    "n_active_measurements",
    "relationships",
]


def materialize_disease_bioactivity(conn: Connection) -> None:
    """Attribute each disease's bridging assays to the bioactivity they measure."""
    evidence, name_map = build_bridge_evidence(conn)
    if evidence.empty:
        logger.info("No bioactivity-disease inputs to materialize (skipping).")
        return

    assay_bio = assay_bioactivity_map(conn)
    if assay_bio.empty:
        logger.info("No assay-bioactivity classifications found (skipping).")
        return

    evidence = evidence.merge(assay_bio, on="source_assay_id", how="inner")
    if evidence.empty:
        logger.info("No bridging assay carried a bioactivity (skipping).")
        return

    result = _aggregate(evidence, name_map)
    bulk_copy(conn, "mv_disease_bioactivity", result, _MV_COLUMNS)
    logger.info("Disease-bioactivity: %d rows.", len(result))


def _aggregate(evidence: pd.DataFrame, name_map: dict) -> pd.DataFrame:
    """Collapse to one row per (disease, bioactivity, chemical)."""
    keys = ["disease_id", "bioactivity_id", "chemical_id"]
    out = (
        evidence.groupby(keys)
        .agg(
            n_assays=("source_assay_id", "nunique"),
            n_active_measurements=("bm", "nunique"),
        )
        .join(_relationships_per_row(evidence, keys))
        .reset_index()
    )
    out["relationships"] = out["relationships"].apply(
        lambda v: v if isinstance(v, list) else []
    )
    for src, dest in (
        ("disease_id", "disease_name"),
        ("bioactivity_id", "bioactivity_name"),
        ("chemical_id", "chemical_name"),
    ):
        out[dest] = out[src].map(name_map)
    out = out.rename(
        columns={
            "disease_id": "disease_foodatlas_id",
            "bioactivity_id": "bioactivity_foodatlas_id",
            "chemical_id": "chemical_foodatlas_id",
        }
    )
    named = out["disease_name"].notna() & out["bioactivity_name"].notna()
    return out[named & out["chemical_name"].notna()]


def _relationships_per_row(evidence: pd.DataFrame, keys: list[str]) -> pd.Series:
    """Distinct bridge relationship types (therapeutic, marker/mechanism, …)."""
    return (
        evidence[[*keys, "relationship"]]
        .explode("relationship")
        .dropna(subset=["relationship"])
        .groupby(keys)["relationship"]
        .apply(lambda s: sorted(set(s)))
        .rename("relationships")
    )
