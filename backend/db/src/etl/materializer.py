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

    chem_ids = set(r1["tail_id"])
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

    disease_chem_ids = set(r3r4["head_id"]) & chem_ids
    relevant_disease_ids = set(r3r4[r3r4["head_id"].isin(disease_chem_ids)]["tail_id"])
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
