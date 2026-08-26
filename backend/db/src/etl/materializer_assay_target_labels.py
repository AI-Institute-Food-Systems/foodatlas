"""Build mv_assay_target_labels — gene id → readable protein name.

``base_bioassays.target_name`` is free text supplied per assay, so one gene
accumulates several spellings. For ``NCBIGene: 4780`` the local data carries
five, of which "Nuclear factor erythroid 2-related factor 2" appears 39 times
and the rest at most three. Taking the **modal** name per gene id therefore
picks the canonical spelling rather than whichever row happened to sort first,
and it collapses the Entrez/UniProt split for free: ``UniProt: Q16236`` has the
same modal name, so both ids label identically in the UI.

Consumed by the API when rendering ``target_genes`` on the two
bioactivity-disease views, which store ids only.
"""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)

_MV_COLUMNS = ["gene_id", "label"]


def materialize_assay_target_labels(conn: Connection) -> None:
    """Pick one display label per assay target gene id."""
    assays = pd.read_sql(
        text(
            "SELECT target_entrez_gene, target_uniprot, target_name"
            " FROM base_bioassays WHERE target_name <> ''"
        ),
        conn,
    )
    if assays.empty:
        logger.info("No assay target names to label (skipping).")
        return

    result = _modal_labels(assays)
    if result.empty:
        logger.info("No assay target ids to label (skipping).")
        return

    bulk_copy(conn, "mv_assay_target_labels", result, _MV_COLUMNS)
    logger.info("Assay target labels: %d gene ids.", len(result))


def _modal_labels(assays: pd.DataFrame) -> pd.DataFrame:
    """One row per gene id, carrying its most frequently used target name."""
    pairs = pd.concat(
        [
            assays[["target_entrez_gene", "target_name"]].rename(
                columns={"target_entrez_gene": "gene_id"}
            ),
            assays[["target_uniprot", "target_name"]].rename(
                columns={"target_uniprot": "gene_id"}
            ),
        ]
    )
    pairs = pairs[pairs["gene_id"].astype(str).str.strip() != ""]
    if pairs.empty:
        return pd.DataFrame(columns=_MV_COLUMNS)

    counts = (
        pairs.groupby(["gene_id", "target_name"]).size().rename("n").reset_index()
    )
    # Ties broken by the shorter name, then alphabetically — both deterministic,
    # and the shorter of two equally-common spellings is the less cluttered one
    # ("Histone deacetylase 6" over "histone deacetylase 6 (3.5.1.-…) [Homo…]").
    counts["length"] = counts["target_name"].str.len()
    counts = counts.sort_values(
        ["gene_id", "n", "length", "target_name"],
        ascending=[True, False, True, True],
    )
    best = counts.drop_duplicates(subset="gene_id", keep="first")
    return best.rename(columns={"target_name": "label"})[_MV_COLUMNS]
