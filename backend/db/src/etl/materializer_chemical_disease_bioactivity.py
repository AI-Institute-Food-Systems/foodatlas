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
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy
from .materializer_bioactivity_bridge import build_bridge_evidence

logger = logging.getLogger(__name__)

_ASSAY_CAP = 25
_GENE_CAP = 50
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
]


def materialize_chemical_disease_bioactivity(conn: Connection) -> None:
    """Infer chemical↔disease associations from shared bioactivity assays."""
    evidence, name_map = build_bridge_evidence(conn)
    if evidence.empty:
        logger.info("No chemical-disease associations inferred (skipping).")
        return

    result = _aggregate(evidence, name_map, _target_gene_map(conn))
    bulk_copy(conn, "mv_chemical_disease_bioactivity", result, _MV_COLUMNS)
    logger.info("Chemical-disease (bioactivity): %d associations.", len(result))


def _target_gene_map(conn: Connection) -> dict[str, list[str]]:
    df = pd.read_sql(
        text(
            "SELECT bioactivity_disease_metadata_id AS bdm, target_ids"
            " FROM base_bioactivity_disease_targets"
        ),
        conn,
    )
    return dict(zip(df["bdm"], df["target_ids"], strict=False))


def _as_list(value: object) -> list:
    return value if isinstance(value, list) else []


def _aggregate(
    evidence: pd.DataFrame, name_map: dict, target_map: dict
) -> pd.DataFrame:
    """Collapse evidence rows to one association per (chemical, disease)."""
    keys = ["chemical_id", "disease_id"]
    base = evidence.groupby(keys).agg(
        n_assays=("source_assay_id", "nunique"),
        n_active_measurements=("bm", "nunique"),
        assays=("source_assay_id", lambda s: sorted(set(s))[:_ASSAY_CAP]),
    )
    rels = (
        evidence[[*keys, "relationship"]]
        .explode("relationship")
        .dropna(subset=["relationship"])
        .groupby(keys)["relationship"]
        .apply(lambda s: sorted(set(s)))
        .rename("relationships")
    )
    genes = _target_genes_per_pair(evidence, target_map, keys)

    out = base.join(rels).join(genes).reset_index()
    out["relationships"] = out["relationships"].apply(_as_list)
    out["target_genes"] = out["target_genes"].apply(_as_list)
    out["chemical_name"] = out["chemical_id"].map(name_map)
    out["disease_name"] = out["disease_id"].map(name_map)
    out = out.rename(
        columns={
            "chemical_id": "chemical_foodatlas_id",
            "disease_id": "disease_foodatlas_id",
        }
    )
    return out[out["chemical_name"].notna() & out["disease_name"].notna()]


def _target_genes_per_pair(
    evidence: pd.DataFrame, target_map: dict, keys: list[str]
) -> pd.Series:
    """Distinct target genes per (chemical, disease), via bdm → target_ids."""
    tmp = evidence[[*keys, "bioactivity_disease_metadata_id"]].explode(
        "bioactivity_disease_metadata_id"
    )
    tmp = tmp.dropna(subset=["bioactivity_disease_metadata_id"])
    tmp["genes"] = tmp["bioactivity_disease_metadata_id"].map(target_map)
    tmp = tmp.explode("genes").dropna(subset=["genes"])
    return (
        tmp.groupby(keys)["genes"]
        .apply(lambda s: sorted(set(s))[:_GENE_CAP])
        .rename("target_genes")
    )
