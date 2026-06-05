"""Helpers shared by materializer_bioactivity for building per-attestation
measurement/evidence payloads.
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection


def name_map(conn: Connection) -> dict[str, str]:
    """Return a map of foodatlas_id → common_name across all base entities."""
    df = pd.read_sql(text("SELECT foodatlas_id, common_name FROM base_entities"), conn)
    return df.set_index("foodatlas_id")["common_name"].to_dict()


def explode_attestations(triplets: pd.DataFrame) -> pd.DataFrame:
    """Explode triplet.attestation_ids into one row per attestation_id."""
    if triplets.empty:
        return triplets.assign(attestation_id=pd.Series(dtype=str))
    df = triplets.explode("attestation_ids").rename(
        columns={"attestation_ids": "attestation_id"}
    )
    return df.dropna(subset=["attestation_id"])


def build_measurement(att: dict) -> dict:
    """Render an attestation row into the measurement/evidence JSON payload."""
    return {
        "attestation_id": att["attestation_id"],
        "bioactivity_metadata_id": att["bioactivity_metadata_id"],
        "source_assay_id": att.get("source_assay_id"),
        "target_ids": att.get("target_ids") or [],
        "potency": {
            "value": att.get("evidence_value_potency_value"),
            "unit": att.get("evidence_value_potency_unit"),
        },
        "hill_curve": {
            "zero_activity": att.get("evidence_value_efficacy_zeroactivity"),
            "infinite_activity": att.get("evidence_value_efficacy_infiniteactivity"),
            "log_ac50": att.get("evidence_value_efficacy_logac50_value"),
            "hill_slope": att.get("evidence_value_efficacy_hillslope"),
        },
        "evidence_source": att.get("evidence_source"),
        "evidence_type": att.get("evidence_type"),
    }
