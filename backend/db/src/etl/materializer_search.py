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
    _materialize_search_auto_complete(conn)
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

    # Count associations per entity
    food_chem_ids = set(r1["tail_id"])
    assoc_counts: dict[str, int] = {}
    for _, row in triplets.iterrows():
        for eid in (row["head_id"], row["tail_id"]):
            assoc_counts[eid] = assoc_counts.get(eid, 0) + 1

    rows = []
    for _, entity in entities.iterrows():
        fid = entity["foodatlas_id"]
        etype = entity["entity_type"]

        # Only include entities that participate in relevant triplets
        if etype == "food" and fid not in set(r1["head_id"]):
            continue
        if etype == "chemical" and fid not in food_chem_ids:
            continue
        if etype == "disease":
            # Only diseases whose correlated chemicals are in food composition
            disease_triplets = r3r4[r3r4["tail_id"] == fid]
            chem_in_food = disease_triplets["head_id"].isin(food_chem_ids)
            if not chem_in_food.any():
                continue

        synonyms = entity["synonyms"] if isinstance(entity["synonyms"], list) else []
        raw_ids = entity["external_ids"]
        ext_ids = raw_ids if isinstance(raw_ids, dict) else {}

        # Build search tokens
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


def _build_exact_tokens(
    entity_type: str,
    common_name: str,
    scientific_name: str,
    synonyms: list[str],
    ext_id_values: list[str],
) -> list[str]:
    """Build exact-match search tokens for an entity."""
    tokens: list[str] = [entity_type, common_name]
    if scientific_name:
        tokens.append(scientific_name)
    tokens.extend(synonyms)
    tokens.extend(ext_id_values)
    return [str(t) for t in tokens]


def _materialize_statistics(conn: Connection) -> None:
    """Compute aggregate statistics for the landing page."""
    stats = [
        ("number of foods", "SELECT COUNT(*) FROM mv_food_entities"),
        ("number of chemicals", "SELECT COUNT(*) FROM mv_chemical_entities"),
        ("number of diseases", "SELECT COUNT(*) FROM mv_disease_entities"),
        (
            "number of associations",
            "SELECT COUNT(*) FROM base_triplets "
            "WHERE relationship_id IN ('r1', 'r3', 'r4')",
        ),
        (
            "number of publications",
            "SELECT COUNT(DISTINCT evidence_id) FROM base_evidence "
            "WHERE source_type = 'pubmed'",
        ),
    ]
    rows = []
    for field, sql in stats:
        result = conn.execute(text(sql))
        count = result.scalar() or 0
        rows.append({"field": field, "count": count})

    if rows:
        df = pd.DataFrame(rows)
        bulk_copy(conn, "mv_metadata_statistics", df, ["field", "count"])
    logger.info("Statistics: %d entries", len(rows))
