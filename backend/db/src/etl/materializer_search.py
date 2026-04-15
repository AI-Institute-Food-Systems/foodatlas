"""Materialize search autocomplete and statistics tables."""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy, truncate_tables

logger = logging.getLogger(__name__)


def refresh_search(conn: Connection) -> None:
    """Truncate and re-populate search + statistics tables."""
    truncate_tables(conn, ["mv_search_auto_complete", "mv_metadata_statistics"])
    logger.info("Building search autocomplete...")
    _materialize_search_auto_complete(conn)
    logger.info("Building metadata statistics...")
    _materialize_statistics(conn)
    conn.commit()


def _materialize_search_auto_complete(conn: Connection) -> None:
    """Build mv_search_auto_complete from entities + triplets."""
    entities = pd.read_sql(text("SELECT * FROM base_entities"), conn)
    triplets = pd.read_sql(
        text("SELECT head_id, tail_id, relationship_id FROM base_triplets"),
        conn,
    )

    r1 = triplets[triplets["relationship_id"] == "r1"]
    r3r4 = triplets[triplets["relationship_id"].isin(["r3", "r4"])]

    # Per-entity association counts mirror what the entity page actually
    # renders: row counts from the already-populated composition and
    # correlation MVs. refresh_search runs after refresh_all (see loader.py),
    # so both tables exist at this point.
    assoc_counts = _load_assoc_counts(conn)

    # Pre-compute relevant entity ID sets
    food_ids = set(r1["head_id"])
    food_chem_ids = set(r1["tail_id"])
    disease_chem_ids = set(r3r4["head_id"]) & food_chem_ids
    relevant_disease_ids = set(r3r4[r3r4["head_id"].isin(disease_chem_ids)]["tail_id"])

    # Filter entities to only those participating in relevant triplets
    relevant = entities[
        ((entities["entity_type"] == "food") & entities["foodatlas_id"].isin(food_ids))
        | (
            (entities["entity_type"] == "chemical")
            & entities["foodatlas_id"].isin(food_chem_ids)
        )
        | (
            (entities["entity_type"] == "disease")
            & entities["foodatlas_id"].isin(relevant_disease_ids)
        )
    ]

    rows = []
    for _, entity in relevant.iterrows():
        fid = entity["foodatlas_id"]
        etype = entity["entity_type"]
        synonyms = entity["synonyms"] if isinstance(entity["synonyms"], list) else []
        raw_ids = entity["external_ids"]
        ext_ids = raw_ids if isinstance(raw_ids, dict) else {}

        ext_id_values = _extract_external_id_values(ext_ids, etype)
        exact_auto = _build_exact_tokens(
            etype,
            entity["common_name"],
            entity["scientific_name"],
            synonyms,
            ext_id_values,
        )
        substr_auto = "    ".join(str(x) for x in exact_auto)

        rows.append(
            {
                "foodatlas_id": fid,
                "associations": assoc_counts.get(fid, 0),
                "entity_type": etype,
                "common_name": entity["common_name"],
                "scientific_name": entity["scientific_name"] or "",
                "synonyms": synonyms,
                "external_ids": ext_ids,
                "exact_auto": exact_auto,
                "substr_auto": substr_auto,
            }
        )

    if not rows:
        return

    result = pd.DataFrame(rows)
    columns = [
        "foodatlas_id",
        "associations",
        "entity_type",
        "common_name",
        "scientific_name",
        "synonyms",
        "external_ids",
        "exact_auto",
        "substr_auto",
    ]
    bulk_copy(conn, "mv_search_auto_complete", result, columns)
    logger.info("Search autocomplete: %d rows", len(result))


def _load_assoc_counts(conn: Connection) -> dict[str, int]:
    """Sum entity row counts across composition and correlation MVs.

    Each MV uses type-specific ID columns (food_/chemical_/disease_foodatlas_id)
    so adding all four groupings never cross-contaminates entity types.
    """
    queries = (
        "SELECT food_foodatlas_id AS fid, COUNT(*) AS n"
        " FROM mv_food_chemical_composition GROUP BY food_foodatlas_id",
        "SELECT chemical_foodatlas_id AS fid, COUNT(*) AS n"
        " FROM mv_food_chemical_composition GROUP BY chemical_foodatlas_id",
        "SELECT chemical_foodatlas_id AS fid, COUNT(*) AS n"
        " FROM mv_chemical_disease_correlation GROUP BY chemical_foodatlas_id",
        "SELECT disease_foodatlas_id AS fid, COUNT(*) AS n"
        " FROM mv_chemical_disease_correlation GROUP BY disease_foodatlas_id",
    )
    counts: dict[str, int] = {}
    for sql in queries:
        for fid, n in conn.execute(text(sql)).all():
            counts[fid] = counts.get(fid, 0) + n
    return counts


def _extract_external_id_values(ext_ids: dict, entity_type: str) -> list[str]:
    """Extract flat list of external ID values for search tokens."""
    values = []
    for key, id_list in ext_ids.items():
        if not isinstance(id_list, list):
            continue
        for each_id in id_list:
            if entity_type == "food" and key == "foodon":
                values.append(str(each_id).split("/")[-1])
            else:
                values.append(str(each_id))
    return values


_MAX_TOKEN_LEN = 200


def _build_exact_tokens(
    entity_type: str,
    common_name: str,
    scientific_name: str,
    synonyms: list[str],
    ext_id_values: list[str],
) -> list[str]:
    """Build exact-match search tokens for an entity.

    Synonyms longer than ``_MAX_TOKEN_LEN`` (e.g. amino acid sequences)
    are excluded to stay within the GIN index page-size limit.
    """
    tokens: list[str] = [entity_type, common_name]
    if scientific_name:
        tokens.append(scientific_name)
    tokens.extend(s for s in synonyms if len(s) <= _MAX_TOKEN_LEN)
    tokens.extend(ext_id_values)
    return [str(t) for t in tokens]


def _materialize_statistics(conn: Connection) -> None:
    """Compute aggregate statistics scoped to the food→chem→disease chain.

    Entities: only those with empirical evidence (r1, scoped r3/r4).
    Associations: empirical edges + IS_A edges from seed entities to root.
    Publications: distinct PMCIDs (food-chem) + distinct PMIDs (scoped CTD).
    """
    triplets = pd.read_sql(
        text("SELECT head_id, tail_id, relationship_id FROM base_triplets"),
        conn,
    )
    entity_types = pd.read_sql(
        text("SELECT foodatlas_id, entity_type FROM base_entities"), conn
    )
    type_map = entity_types.groupby("entity_type")["foodatlas_id"].apply(set).to_dict()

    r1 = triplets[triplets["relationship_id"] == "r1"]
    r2 = triplets[triplets["relationship_id"] == "r2"]
    r3r4 = triplets[triplets["relationship_id"].isin(["r3", "r4"])]

    food_ids = set(r1["head_id"])
    chem_ids = set(r1["tail_id"])
    scoped_r3r4 = r3r4[r3r4["head_id"].isin(chem_ids)]
    disease_ids = set(scoped_r3r4["tail_id"])

    # All r2 triplets use natural direction: head=child, tail=parent.
    assoc_r2 = (
        _count_scoped_r2(r2, food_ids, type_map.get("food", set()))
        + _count_scoped_r2(r2, chem_ids, type_map.get("chemical", set()))
        + _count_scoped_r2(r2, disease_ids, type_map.get("disease", set()))
    )
    associations = len(r1) + len(scoped_r3r4) + assoc_r2

    pubmed_pmcids = (
        conn.execute(
            text(
                "SELECT COUNT(DISTINCT (reference->>'pmcid'))"
                " FROM base_evidence WHERE source_type = 'pubmed'"
            )
        ).scalar()
        or 0
    )
    ctd_pmids = (
        conn.execute(
            text(
                "SELECT COUNT(DISTINCT (e.reference->>'pmid'))"
                " FROM base_triplets t,"
                "  LATERAL unnest(t.attestation_ids) AS att_id"
                " JOIN base_attestations a ON a.attestation_id = att_id"
                " JOIN base_evidence e ON e.evidence_id = a.evidence_id"
                " WHERE t.relationship_id IN ('r3','r4')"
                "  AND t.head_id IN (SELECT DISTINCT tail_id FROM base_triplets"
                "   WHERE relationship_id = 'r1')"
                "  AND e.source_type = 'ctd'"
                "  AND e.reference->>'pmid' IS NOT NULL"
            )
        ).scalar()
        or 0
    )
    publications = pubmed_pmcids + ctd_pmids

    rows = [
        {"field": "number of foods", "count": len(food_ids)},
        {"field": "number of chemicals", "count": len(chem_ids)},
        {"field": "number of diseases", "count": len(disease_ids)},
        {"field": "number of associations", "count": associations},
        {"field": "number of publications", "count": publications},
    ]
    df = pd.DataFrame(rows)
    bulk_copy(conn, "mv_metadata_statistics", df, ["field", "count"])
    logger.info("Statistics: %d entries", len(rows))


def _count_scoped_r2(
    r2: pd.DataFrame,
    seed_ids: set[str],
    type_ids: set[str],
) -> int:
    """Count IS_A edges reachable from seed entities to root.

    All r2 triplets use natural direction: head=child, tail=parent.
    """
    typed_r2 = r2[r2["head_id"].isin(type_ids) & r2["tail_id"].isin(type_ids)]
    parents_of: dict[str, set[str]] = {}
    for _, row in typed_r2.iterrows():
        parents_of.setdefault(row["head_id"], set()).add(row["tail_id"])

    reachable = set(seed_ids)
    stack = list(seed_ids)
    while stack:
        node = stack.pop()
        for parent in parents_of.get(node, set()):
            if parent not in reachable:
                reachable.add(parent)
                stack.append(parent)

    scoped = typed_r2[
        typed_r2["head_id"].isin(reachable) & typed_r2["tail_id"].isin(reachable)
    ]
    return len(scoped)
