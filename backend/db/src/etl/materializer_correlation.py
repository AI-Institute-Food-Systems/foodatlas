"""Materialize chemical-disease correlation table."""

import json
import logging
from collections import defaultdict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection
from tqdm import tqdm

from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)

_PUBMED_URL = "https://pubmed.ncbi.nlm.nih.gov/"
_PMC_URL = "https://www.ncbi.nlm.nih.gov/pmc/?term="


def materialize_chemical_disease_correlation(conn: Connection) -> None:
    """Build mv_chemical_disease_correlation from r3/r4 triplets.

    For each chemical, includes disease correlations from itself and all
    descendant chemicals (via IS_A hierarchy) so that parent categories
    like "Vitamin" surface their children's connections.

    Each (chemical, source_chemical, disease, rel) gets its own row so
    the frontend can show which descendant contributes the connection.
    """
    r3r4 = pd.read_sql(
        text("SELECT * FROM base_triplets WHERE relationship_id IN ('r3', 'r4')"),
        conn,
    )
    attestations = pd.read_sql(text("SELECT * FROM base_attestations"), conn)
    evidence = pd.read_sql(text("SELECT * FROM base_evidence"), conn)
    entities = pd.read_sql(
        text("SELECT foodatlas_id, common_name, entity_type FROM base_entities"),
        conn,
    )

    name_map = entities.set_index("foodatlas_id")["common_name"].to_dict()
    etype_map = entities.set_index("foodatlas_id")["entity_type"].to_dict()
    att_map = attestations.set_index("attestation_id")
    ev_map = evidence.set_index("evidence_id")

    ancestors_of = _build_ancestors(conn, etype_map)

    # Keyed by (chemical_id, source_chemical_id, disease_id, rel_id)
    agg: dict[tuple, dict] = {}

    for _, triplet in tqdm(
        r3r4.iterrows(), total=len(r3r4), desc="correlation", leave=True
    ):
        chem_id = triplet["head_id"]
        disease_id = triplet["tail_id"]
        rel_id = triplet["relationship_id"]
        att_ids = triplet["attestation_ids"] or []

        sources, evidences = _get_correlation_evidence(att_ids, att_map, ev_map)

        # Direct row: source_chemical = chemical itself
        _merge_into(agg, chem_id, chem_id, disease_id, rel_id, sources, evidences)

        # Inherited rows: source_chemical = the actual descendant (chem_id)
        for ancestor_id in ancestors_of.get(chem_id, set()):
            _merge_into(
                agg, ancestor_id, chem_id, disease_id, rel_id, sources, evidences
            )

    if not agg:
        return

    rows = []
    for (chem_id, src_chem_id, disease_id, rel_id), data in agg.items():
        deduped = _deduplicate_evidences(data["evidences"])
        rows.append(
            {
                "chemical_name": name_map.get(chem_id, ""),
                "chemical_foodatlas_id": chem_id,
                "relationship_id": rel_id,
                "disease_name": name_map.get(disease_id, ""),
                "disease_foodatlas_id": disease_id,
                "source_chemical_name": name_map.get(src_chem_id, ""),
                "source_chemical_foodatlas_id": src_chem_id,
                "sources": list(data["sources"]),
                "evidences": json.dumps(deduped),
                "evidence_count": len(deduped),
            }
        )

    result = pd.DataFrame(rows)
    columns = [
        "chemical_name",
        "chemical_foodatlas_id",
        "relationship_id",
        "disease_name",
        "disease_foodatlas_id",
        "source_chemical_name",
        "source_chemical_foodatlas_id",
        "sources",
        "evidences",
        "evidence_count",
    ]
    bulk_copy(conn, "mv_chemical_disease_correlation", result, columns)
    logger.info("Chemical-disease correlations: %d rows", len(result))


def _merge_into(
    agg: dict[tuple, dict],
    chem_id: str,
    src_chem_id: str,
    disease_id: str,
    rel_id: str,
    sources: list[str],
    evidences: list[dict],
) -> None:
    """Merge evidence into aggregated row."""
    key = (chem_id, src_chem_id, disease_id, rel_id)
    if key not in agg:
        agg[key] = {"sources": set(), "evidences": []}
    agg[key]["sources"].update(sources)
    agg[key]["evidences"].extend(evidences)


def _deduplicate_evidences(evidences: list[dict]) -> list[dict]:
    """Remove duplicate evidence entries by PMID/PMCID."""
    seen: set[str] = set()
    result: list[dict] = []
    for ev in evidences:
        eid = ev.get("pmid", {}).get("id") or ev.get("pmcid", {}).get("id") or ""
        key = str(eid)
        if key and key not in seen:
            seen.add(key)
            result.append(ev)
    return result


def _build_ancestors(
    conn: Connection, etype_map: dict[str, str]
) -> dict[str, set[str]]:
    """Build child -> all ancestors map from chemical IS_A triplets."""
    r2 = pd.read_sql(
        text("SELECT head_id, tail_id FROM base_triplets WHERE relationship_id = 'r2'"),
        conn,
    )
    # Filter to chemical-chemical IS_A only
    r2 = r2[
        r2["head_id"].map(lambda x: etype_map.get(x) == "chemical")
        & r2["tail_id"].map(lambda x: etype_map.get(x) == "chemical")
    ]

    parents_of: dict[str, set[str]] = defaultdict(set)
    for _, row in r2.iterrows():
        parents_of[row["head_id"]].add(row["tail_id"])

    cache: dict[str, set[str]] = {}

    def _ancestors(node: str) -> set[str]:
        if node in cache:
            return cache[node]
        result: set[str] = set()
        stack = list(parents_of.get(node, set()))
        while stack:
            parent = stack.pop()
            if parent not in result:
                result.add(parent)
                stack.extend(parents_of.get(parent, set()))
        cache[node] = result
        return result

    for node in parents_of:
        _ancestors(node)

    return cache


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
                "url": f"{_PMC_URL}{ref['pmcid']}",
            }
        if "pmid" in ref:
            ev_dict["pmid"] = {
                "id": str(ref["pmid"]),
                "url": f"{_PUBMED_URL}{ref['pmid']}",
            }
        if ev_dict:
            evidences.append(ev_dict)

    return list(sources), evidences
