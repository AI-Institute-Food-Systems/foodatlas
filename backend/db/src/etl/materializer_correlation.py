"""Materialize chemical-disease correlation table."""

import json
import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection
from tqdm import tqdm

from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)


def materialize_chemical_disease_correlation(conn: Connection) -> None:
    """Build mv_chemical_disease_correlation from r3/r4 triplets."""
    r3r4 = pd.read_sql(
        text("SELECT * FROM base_triplets WHERE relationship_id IN ('r3', 'r4')"),
        conn,
    )
    attestations = pd.read_sql(text("SELECT * FROM base_attestations"), conn)
    evidence = pd.read_sql(text("SELECT * FROM base_evidence"), conn)
    entities = pd.read_sql(
        text("SELECT foodatlas_id, common_name FROM base_entities"), conn
    )

    name_map = entities.set_index("foodatlas_id")["common_name"].to_dict()
    att_map = attestations.set_index("attestation_id")
    ev_map = evidence.set_index("evidence_id")

    rows = []
    for _, triplet in tqdm(
        r3r4.iterrows(), total=len(r3r4), desc="correlation", leave=True
    ):
        chem_id = triplet["head_id"]
        disease_id = triplet["tail_id"]
        att_ids = triplet["attestation_ids"] or []

        sources, evidences = _get_correlation_evidence(att_ids, att_map, ev_map)
        rows.append(
            {
                "chemical_name": name_map.get(chem_id, ""),
                "chemical_foodatlas_id": chem_id,
                "relationship_id": triplet["relationship_id"],
                "disease_name": name_map.get(disease_id, ""),
                "disease_foodatlas_id": disease_id,
                "sources": sources,
                "evidences": json.dumps(evidences),
            }
        )

    if not rows:
        return

    result = pd.DataFrame(rows)
    columns = [
        "chemical_name",
        "chemical_foodatlas_id",
        "relationship_id",
        "disease_name",
        "disease_foodatlas_id",
        "sources",
        "evidences",
    ]
    bulk_copy(conn, "mv_chemical_disease_correlation", result, columns)
    logger.info("Chemical-disease correlations: %d rows", len(result))


def _get_correlation_evidence(
    att_ids: list[str],
    att_map: pd.DataFrame,
    ev_map: pd.DataFrame,
) -> tuple[list[str], list[dict]]:
    """Extract sources and PMID/PMCID evidence for correlations."""
    sources: set[str] = set()
    evidences: list[dict] = []
    for att_id in att_ids:
        if att_id not in att_map.index:
            continue
        att = att_map.loc[att_id]
        sources.add(att["source"])

        ev_id = att["evidence_id"]
        if ev_id not in ev_map.index:
            continue
        ref = ev_map.loc[ev_id]["reference"]
        if isinstance(ref, str):
            ref = json.loads(ref)

        ev_dict: dict = {}
        if "pmcid" in ref:
            ev_dict["pmcid"] = {
                "id": ref["pmcid"],
                "url": f"https://www.ncbi.nlm.nih.gov/pmc/?term={ref['pmcid']}",
            }
        if "pmid" in ref:
            ev_dict["pmid"] = {
                "id": ref["pmid"],
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{ref['pmid']}",
            }
        if ev_dict:
            evidences.append(ev_dict)

    return list(sources), evidences
