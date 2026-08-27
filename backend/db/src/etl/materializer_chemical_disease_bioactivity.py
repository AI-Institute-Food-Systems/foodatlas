"""Build mv_chemical_disease_bioactivity — chemical↔disease from shared assays.

Inference: a chemical is associated with a disease when it has ≥1 **Active**
measurement in an assay that the bioactivity-disease bridge ties to that disease.

    r6 (chemical measured for bioactivity) --attestation_ids--> bm measurements
    bm (Active) --source_assay_id--> assay
    assay --bridge--> disease (MeSH -> KG disease via external_ids.ctd)
    bridge --bdm--> target genes

Aggregated to one row per (chemical, disease) with the shared assays, target
genes, relationship type(s), and counts. Distinct from CTD literature
correlations (mv_chemical_disease_correlation).

The graph walk itself lives in ``materializer_bioactivity_bridge`` — shared
with ``mv_disease_bioactivity``, which keeps the bridging assay's bioactivity
instead of collapsing it away.
"""

import logging

import pandas as pd
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy
from .materializer_bioactivity_bridge import (
    ASSAY_CAP,
    as_list,
    attach_literature,
    build_bridge_evidence,
    literature_directions,
    target_gene_map,
    target_genes_per_pair,
)

logger = logging.getLogger(__name__)

_MV_COLUMNS = [
    "chemical_name",
    "chemical_foodatlas_id",
    "disease_name",
    "disease_foodatlas_id",
    "n_assays",
    "n_active_measurements",
    "relationships",
    "target_genes",
    "assays",
    "literature_directions",
]


def materialize_chemical_disease_bioactivity(conn: Connection) -> None:
    """Infer chemical↔disease associations from shared bioactivity assays."""
    evidence, name_map = build_bridge_evidence(conn)
    if evidence.empty:
        logger.info("No chemical-disease associations inferred (skipping).")
        return

    result = _aggregate(
        evidence, name_map, target_gene_map(conn), literature_directions(conn)
    )
    bulk_copy(conn, "mv_chemical_disease_bioactivity", result, _MV_COLUMNS)
    logger.info("Chemical-disease (bioactivity): %d associations.", len(result))


def _aggregate(
    evidence: pd.DataFrame, name_map: dict, target_map: dict, lit: pd.DataFrame
) -> pd.DataFrame:
    """Collapse evidence rows to one association per (chemical, disease)."""
    keys = ["chemical_id", "disease_id"]
    base = evidence.groupby(keys).agg(
        n_assays=("source_assay_id", "nunique"),
        n_active_measurements=("bm", "nunique"),
        assays=("source_assay_id", lambda s: sorted(set(s))[:ASSAY_CAP]),
    )
    rels = (
        evidence[[*keys, "relationship"]]
        .explode("relationship")
        .dropna(subset=["relationship"])
        .groupby(keys)["relationship"]
        .apply(lambda s: sorted(set(s)))
        .rename("relationships")
    )
    genes = target_genes_per_pair(evidence, target_map, keys)

    out = base.join(rels).join(genes).reset_index()
    out["relationships"] = out["relationships"].apply(as_list)
    out["target_genes"] = out["target_genes"].apply(as_list)
    out = attach_literature(out, lit)
    out["chemical_name"] = out["chemical_id"].map(name_map)
    out["disease_name"] = out["disease_id"].map(name_map)
    out = out.rename(
        columns={
            "chemical_id": "chemical_foodatlas_id",
            "disease_id": "disease_foodatlas_id",
        }
    )
    return out[out["chemical_name"].notna() & out["disease_name"].notna()]
