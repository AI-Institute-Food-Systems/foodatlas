"""Materialize denormalized API tables from base tables."""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy, truncate_tables
from .materializer_composition import materialize_food_chemical_composition
from .materializer_correlation import materialize_chemical_disease_correlation

logger = logging.getLogger(__name__)
MV_TABLES = [
    "mv_food_entities",
    "mv_chemical_entities",
    "mv_disease_entities",
    "mv_food_chemical_composition",
    "mv_chemical_disease_correlation",
]


def refresh_all(conn: Connection) -> None:
    """Truncate and re-populate all materialized API tables."""
    truncate_tables(conn, MV_TABLES)
    logger.info("Building entity views...")
    _materialize_entity_views(conn)
    logger.info("Building food-chemical composition...")
    materialize_food_chemical_composition(conn)
    logger.info("Building chemical-disease correlation...")
    materialize_chemical_disease_correlation(conn)
    conn.commit()


def _materialize_entity_views(conn: Connection) -> None:
    """Compute mv_food_entities, mv_chemical_entities, mv_disease_entities."""
    entities = pd.read_sql(text("SELECT * FROM base_entities"), conn)
    triplets = pd.read_sql(text("SELECT * FROM base_triplets"), conn)

    r1 = triplets[triplets["relationship_id"] == "r1"]
    r3r4 = triplets[triplets["relationship_id"].isin(["r3", "r4"])]

    food_ids = set(r1["head_id"])
    foods = entities[
        (entities["entity_type"] == "food") & (entities["foodatlas_id"].isin(food_ids))
    ].copy()
    foods["food_classification"] = foods["attributes"].apply(
        lambda a: a.get("food_groups", []) if isinstance(a, dict) else []
    )
    _insert_mv_entities(conn, "mv_food_entities", foods, ["food_classification"])

    # Include chemicals from food composition (r1), disease correlations (r3/r4),
    # and their IS_A ancestors (so ancestor pages have metadata).
    r2 = triplets[triplets["relationship_id"] == "r2"]
    disease_chem_ids = set(r3r4["head_id"])
    ancestor_ids = _collect_ancestors(r2, disease_chem_ids, entities)
    chem_ids = set(r1["tail_id"]) | disease_chem_ids | ancestor_ids
    chemicals = entities[
        (entities["entity_type"] == "chemical")
        & (entities["foodatlas_id"].isin(chem_ids))
    ].copy()
    chemicals["chemical_classification"] = chemicals["attributes"].apply(
        lambda a: a.get("chemical_groups", []) if isinstance(a, dict) else []
    )
    chemicals["flavor_descriptors"] = chemicals["attributes"].apply(
        lambda a: a.get("flavor_descriptors", []) if isinstance(a, dict) else []
    )
    _insert_mv_entities(
        conn,
        "mv_chemical_entities",
        chemicals,
        ["chemical_classification", "flavor_descriptors"],
    )

    relevant_disease_ids = set(r3r4["tail_id"])
    diseases = entities[
        (entities["entity_type"] == "disease")
        & (entities["foodatlas_id"].isin(relevant_disease_ids))
    ].copy()
    _insert_mv_entities(conn, "mv_disease_entities", diseases, [])

    logger.info(
        "Entity views: %d foods, %d chemicals, %d diseases",
        len(foods),
        len(chemicals),
        len(diseases),
    )


def _collect_ancestors(
    r2: pd.DataFrame, seed_ids: set[str], entities: pd.DataFrame
) -> set[str]:
    """Return all chemical ancestors of seed_ids via IS_A (r2) triplets.

    Chemical-chemical r2 uses head=parent, tail=child (see
    backend/api/src/repositories/taxonomy.py), so a child's parents are the
    head_ids of rows where it appears as tail.
    """
    chem_ids_all = set(entities[entities["entity_type"] == "chemical"]["foodatlas_id"])
    chem_r2 = r2[r2["head_id"].isin(chem_ids_all) & r2["tail_id"].isin(chem_ids_all)]
    parents_of: dict[str, set[str]] = {}
    for _, row in chem_r2.iterrows():
        parents_of.setdefault(row["tail_id"], set()).add(row["head_id"])

    ancestors: set[str] = set()
    for node in seed_ids:
        stack = list(parents_of.get(node, set()))
        while stack:
            parent = stack.pop()
            if parent not in ancestors:
                ancestors.add(parent)
                stack.extend(parents_of.get(parent, set()))
    return ancestors


def _insert_mv_entities(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    extra_cols: list[str],
) -> None:
    """Insert entity DataFrame into a materialized view table."""
    base_cols = [
        "foodatlas_id",
        "entity_type",
        "common_name",
        "scientific_name",
        "synonyms",
        "external_ids",
    ]
    bulk_copy(conn, table_name, df, base_cols + extra_cols)
