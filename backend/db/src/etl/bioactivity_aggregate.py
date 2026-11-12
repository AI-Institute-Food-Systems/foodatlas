"""Per-pair aggregation for the chemical/food bioactivity MVs."""

from collections import defaultdict

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

MEASUREMENTS_CAP = 25  # per pair, for display


def _num(x: float | None) -> float | None:
    """JSON-safe float: NaN/None → None (the efficacy/fit columns are nullable)."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return float(x)


def _label(x: object) -> str:
    """A text column's value, or "" for None/NaN."""
    return x if isinstance(x, str) else ""


def source_kind(src: str | None) -> str:
    """Bucket a measurement's ``evidence_source`` for the Assay Source filter."""
    s = (src or "").lower()
    if s.startswith("exp"):
        return "experimental"
    if s.startswith(("pred", "comp")):
        return "predicted"
    return ""


def assay_meta_map(conn: Connection) -> dict[str, dict]:
    """source_assay_id → nested assay metadata (empty when base_bioassays is empty)."""
    df = pd.read_sql(
        text(
            "SELECT source_assay_id, source, assay_description, target_name,"
            " target_organism, target_uniprot, target_entrez_gene, n_measurements"
            " FROM base_bioassays"
        ),
        conn,
    )
    return {
        r.source_assay_id: {
            "source": r.source,
            "description": r.assay_description,
            "target_name": r.target_name,
            "target_organism": r.target_organism,
            "target_uniprot": r.target_uniprot,
            "target_entrez_gene": r.target_entrez_gene,
            "n_measurements": (
                None if pd.isna(r.n_measurements) else int(r.n_measurements)
            ),
        }
        for r in df.itertuples(index=False)
    }


def aggregate_pair(rows: list, assay_meta: dict[str, dict]) -> dict:
    """One pass over a pair's measurement rows: counts and a display sample.

    No cross-assay potency rollup: each measurement is a distinct assay
    (target/cell line/protocol), so a median over them is not a meaningful
    potency. The per-assay values live in ``measurements`` (capped) and in
    ``base_attestations_bioactivity`` (complete). Each sampled measurement is
    enriched with its assay's metadata (``assay_meta``), joined by assay id.

    ``evidence_types`` and ``units`` cover every row, not only the sample, so
    the API's Evidence and Unit filters see every assay of the pair.
    """
    outcome_counts: dict[str, int] = defaultdict(int)
    source_counts: dict[str, int] = defaultdict(int)
    evidence_types: set[str] = set()
    units: set[str] = set()
    sample: list[dict] = []
    for i, r in enumerate(rows):
        outcome = r.reported_activity_outcome
        outcome_counts[outcome] += 1
        source_counts[source_kind(r.evidence_source)] += 1
        evidence_types.add(_label(r.evidence_type))
        units.add(_label(r.potency_unit))
        if i < MEASUREMENTS_CAP:
            sample.append(
                {
                    "bioactivity_metadata_id": r.bioactivity_metadata_id,
                    "exhibit_type": r.exhibit_type,
                    "assay": r.source_assay_id,
                    "outcome": outcome,
                    "endpoint": r.evidence_endpoint_type,
                    "relation": r.evidence_relation,
                    "value": _num(r.potency_value),
                    "unit": r.potency_unit,
                    "efficacy_zeroactivity": _num(r.efficacy_zeroactivity),
                    "efficacy_infiniteactivity": _num(r.efficacy_infiniteactivity),
                    "efficacy_logac50_value": _num(r.efficacy_logac50_value),
                    "efficacy_hillslope": _num(r.efficacy_hillslope),
                    "evidence_source": r.evidence_source,
                    "evidence_type": r.evidence_type,
                    "evidence_fit_r2": _num(r.evidence_fit_r2),
                    "evidence_fit_curveclass": r.evidence_fit_curveclass,
                    "assay_meta": assay_meta.get(r.source_assay_id),
                }
            )
    return {
        "measurement_count": len(rows),
        "active_count": outcome_counts["Active"],
        "inactive_count": outcome_counts["Inactive"],
        "unspecified_count": outcome_counts["Unspecified"],
        "inconclusive_count": outcome_counts["Inconclusive"],
        "experimental_count": source_counts["experimental"],
        "predicted_count": source_counts["predicted"],
        "evidence_types": sorted(evidence_types - {""}),
        "units": sorted(units - {""}),
        "measurements": sample,
    }
