"""Materialize chemical-disease correlation table."""

import json
import logging
from collections import defaultdict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)

_PUBMED_URL = "https://pubmed.ncbi.nlm.nih.gov/"
_PMC_URL = "https://www.ncbi.nlm.nih.gov/pmc/?term="


def materialize_chemical_disease_correlation(conn: Connection) -> None:
    """Build mv_chemical_disease_correlation from r3/r4 triplets.

    For each chemical, includes disease correlations from itself and all
    descendant chemicals (via IS_A hierarchy) so that parent categories
    like "Vitamin" surface their children's connections. Inherited rows
    only fire when the descendant is food-connected (appears as an r1
    tail); purely ontological descendants with no food presence would
    otherwise flood parent pages with irrelevant disease rows.

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
    food_chem_ids = set(
        pd.read_sql(
            text(
                "SELECT DISTINCT tail_id FROM base_triplets"
                " WHERE relationship_id = 'r1'"
            ),
            conn,
        )["tail_id"]
    )

    name_map = entities.set_index("foodatlas_id")["common_name"].to_dict()
    etype_map = entities.set_index("foodatlas_id")["entity_type"].to_dict()
    # Pure-dict lookups are ~100x faster than pandas .loc in tight loops.
    att_dict = attestations.set_index("attestation_id").to_dict("index")
    ev_dict = evidence.set_index("evidence_id")["reference"].to_dict()

    ancestors_of = _build_ancestors(conn, etype_map)

    rows: list[dict] = []
    for head_id, tail_id, rel_id, att_ids in zip(
        r3r4["head_id"],
        r3r4["tail_id"],
        r3r4["relationship_id"],
        r3r4["attestation_ids"],
        strict=False,
    ):
        sources, evidences = _extract_evidence(att_ids or [], att_dict, ev_dict)
        deduped = _deduplicate_evidences(evidences)
        evidences_json = json.dumps(deduped)
        source_list = list(sources)

        # Direct row: source_chemical = chemical itself. Always emitted
        # so the chemical's own page shows its own disease edges.
        rows.append(
            _make_row(
                head_id,
                head_id,
                tail_id,
                rel_id,
                source_list,
                evidences_json,
                len(deduped),
                name_map,
            )
        )
        # Inherited rows — only propagate disease edges up the ChEBI tree
        # when the descendant itself is food-connected. A purely
        # ontological descendant with disease data but no food presence
        # is not useful on a food-chemical-disease page.
        if head_id not in food_chem_ids:
            continue
        for ancestor_id in ancestors_of.get(head_id, ()):
            rows.append(
                _make_row(
                    ancestor_id,
                    head_id,
                    tail_id,
                    rel_id,
                    source_list,
                    evidences_json,
                    len(deduped),
                    name_map,
                )
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
        "source_chemical_name",
        "source_chemical_foodatlas_id",
        "sources",
        "evidences",
        "evidence_count",
    ]
    bulk_copy(conn, "mv_chemical_disease_correlation", result, columns)
    logger.info("Chemical-disease correlations: %d rows", len(result))


def _make_row(
    chem_id: str,
    src_chem_id: str,
    disease_id: str,
    rel_id: str,
    sources: list[str],
    evidences_json: str,
    evidence_count: int,
    name_map: dict[str, str],
) -> dict:
    return {
        "chemical_name": name_map.get(chem_id, ""),
        "chemical_foodatlas_id": chem_id,
        "relationship_id": rel_id,
        "disease_name": name_map.get(disease_id, ""),
        "disease_foodatlas_id": disease_id,
        "source_chemical_name": name_map.get(src_chem_id, ""),
        "source_chemical_foodatlas_id": src_chem_id,
        "sources": sources,
        "evidences": evidences_json,
        "evidence_count": evidence_count,
    }


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
    """Build child -> all ancestors map from chemical IS_A triplets.

    Chemical-chemical r2 uses head=parent, tail=child (see
    backend/api/src/repositories/taxonomy.py), so a child's parents are the
    head_ids of rows where it appears as tail.
    """
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
        parents_of[row["tail_id"]].add(row["head_id"])

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


def _extract_evidence(
    att_ids: list[str],
    att_dict: dict[str, dict],
    ev_dict: dict[str, object],
) -> tuple[set[str], list[dict]]:
    """Extract sources and PMID/PMCID evidence from plain-dict lookups."""
    sources: set[str] = set()
    evidences: list[dict] = []
    for att_id in att_ids:
        att = att_dict.get(att_id)
        if att is None:
            continue
        sources.add(att["source"])

        ref_raw = ev_dict.get(att["evidence_id"])
        if ref_raw is None:
            continue
        if isinstance(ref_raw, str):
            ref: dict = json.loads(ref_raw)
        elif isinstance(ref_raw, dict):
            ref = ref_raw
        else:
            continue

        ev_dict_row: dict = {}
        if "pmcid" in ref:
            ev_dict_row["pmcid"] = {
                "id": ref["pmcid"],
                "url": f"{_PMC_URL}{ref['pmcid']}",
            }
        if "pmid" in ref:
            ev_dict_row["pmid"] = {
                "id": str(ref["pmid"]),
                "url": f"{_PUBMED_URL}{ref['pmid']}",
            }
        if ev_dict_row:
            evidences.append(ev_dict_row)
    return sources, evidences


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
