"""Build the bioactivity materialized views (concept / chemical / food).

Mirrors :mod:`materializer_composition`: explode ``r5``/``r6`` triplets'
``attestation_ids``, join the bioactivity measurement table, group per
(entity, bioactivity) pair in pure Python, and pre-build the JSON the API
serves. Plus ``mv_bioactivity_entities`` for the concept pages (built from the
bioactivity entities + the ``r2`` concept hierarchy).

Skips cleanly when the KG has no bioactivity (the tables exist but stay empty).
"""

import json
import logging
from collections import defaultdict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bioactivity_aggregate import aggregate_pair, assay_meta_map
from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)


def materialize_bioactivity(conn: Connection) -> None:
    """Build all three bioactivity MVs from the base tables."""
    name_map = _name_map(conn)
    assay_meta = assay_meta_map(conn)
    materialize_chemical_bioactivity(conn, name_map, assay_meta)
    materialize_food_bioactivity(conn, name_map, assay_meta)
    materialize_bioactivity_entities(conn, name_map)


def _name_map(conn: Connection) -> dict[str, str]:
    df = pd.read_sql(text("SELECT foodatlas_id, common_name FROM base_entities"), conn)
    result: dict[str, str] = df.set_index("foodatlas_id")["common_name"].to_dict()
    return result


def _exploded_measurements(conn: Connection, rel_id: str) -> pd.DataFrame:
    """Explode r5/r6 triplets and inner-join the bioactivity measurements."""
    triplets = pd.read_sql(
        text(
            "SELECT head_id, tail_id, attestation_ids FROM base_triplets"
            " WHERE relationship_id = :rel"
        ),
        conn,
        params={"rel": rel_id},
    )
    if triplets.empty:
        return pd.DataFrame()
    measurements = pd.read_sql(
        # every column — the API exposes the full measurement record
        text("SELECT * FROM base_attestations_bioactivity"),
        conn,
    )
    exploded = triplets.explode("attestation_ids").rename(
        columns={"attestation_ids": "bm"}
    )
    exploded = exploded.dropna(subset=["bm"])
    return exploded.merge(
        measurements, left_on="bm", right_on="bioactivity_metadata_id", how="inner"
    )


def _group_by_pair(merged: pd.DataFrame) -> dict[tuple[str, str], list]:
    """Group merged rows by (head_id, tail_id) in pure Python (many small groups)."""
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for row in merged.itertuples(index=False):
        groups[(row.head_id, row.tail_id)].append(row)
    return groups


def materialize_chemical_bioactivity(
    conn: Connection, name_map: dict[str, str], assay_meta: dict[str, dict]
) -> None:
    """One row per (chemical, bioactivity) from r6 + measurements.

    Also stamps each row with n_foods (distinct foods containing this
    chemical) by joining the already-populated mv_food_chemical_composition.
    Pre-materialising the count lets /bioactivity/chemicals sort by it
    in O(log n) rather than via the O(n*log n) correlated-subquery
    approach we shipped initially.
    """
    merged = _exploded_measurements(conn, "r6")
    if merged.empty:
        return

    # Pre-aggregate distinct-foods-per-chemical from FCC. Runs once at
    # build time; the API never has to recompute it.
    n_foods_df = pd.read_sql(
        text(
            "SELECT chemical_foodatlas_id, "
            "COUNT(DISTINCT food_foodatlas_id) AS n_foods "
            "FROM mv_food_chemical_composition "
            "GROUP BY chemical_foodatlas_id"
        ),
        conn,
    )
    n_foods_map = dict(
        zip(n_foods_df["chemical_foodatlas_id"], n_foods_df["n_foods"], strict=False)
    )

    result_rows = []
    for (chem_id, bio_id), rows in _group_by_pair(merged).items():
        agg = aggregate_pair(rows, assay_meta)
        result_rows.append(
            {
                "chemical_name": name_map.get(chem_id, ""),
                "chemical_foodatlas_id": chem_id,
                "bioactivity_name": name_map.get(bio_id, ""),
                "bioactivity_foodatlas_id": bio_id,
                "measurement_count": agg["measurement_count"],
                "active_count": agg["active_count"],
                "inactive_count": agg["inactive_count"],
                "unspecified_count": agg["unspecified_count"],
                "inconclusive_count": agg["inconclusive_count"],
                "experimental_count": agg["experimental_count"],
                "predicted_count": agg["predicted_count"],
                "evidence_types": agg["evidence_types"],
                "units": agg["units"],
                "n_foods": int(n_foods_map.get(chem_id, 0)),
                "measurements": json.dumps(agg["measurements"]),
            }
        )
    df = pd.DataFrame(result_rows)
    bulk_copy(
        conn,
        "mv_chemical_bioactivity",
        df,
        [
            "chemical_name",
            "chemical_foodatlas_id",
            "bioactivity_name",
            "bioactivity_foodatlas_id",
            "measurement_count",
            "active_count",
            "inactive_count",
            "unspecified_count",
            "inconclusive_count",
            "experimental_count",
            "predicted_count",
            "evidence_types",
            "units",
            "n_foods",
            "measurements",
        ],
    )
    logger.info("Chemical-bioactivity: %d rows", len(df))


def materialize_food_bioactivity(
    conn: Connection, name_map: dict[str, str], assay_meta: dict[str, dict]
) -> None:
    """One row per (food, bioactivity) from r5 + measurements."""
    merged = _exploded_measurements(conn, "r5")
    if merged.empty:
        return
    result_rows = []
    for (food_id, bio_id), rows in _group_by_pair(merged).items():
        agg = aggregate_pair(rows, assay_meta)
        result_rows.append(
            {
                "food_name": name_map.get(food_id, ""),
                "food_foodatlas_id": food_id,
                "bioactivity_name": name_map.get(bio_id, ""),
                "bioactivity_foodatlas_id": bio_id,
                "measurement_count": agg["measurement_count"],
                "experimental_count": agg["experimental_count"],
                "predicted_count": agg["predicted_count"],
                "evidence_types": agg["evidence_types"],
                "units": agg["units"],
                "measurements": json.dumps(agg["measurements"]),
            }
        )
    df = pd.DataFrame(result_rows)
    bulk_copy(
        conn,
        "mv_food_bioactivity",
        df,
        [
            "food_name",
            "food_foodatlas_id",
            "bioactivity_name",
            "bioactivity_foodatlas_id",
            "measurement_count",
            "experimental_count",
            "predicted_count",
            "evidence_types",
            "units",
            "measurements",
        ],
    )
    logger.info("Food-bioactivity: %d rows", len(df))


def materialize_bioactivity_entities(
    conn: Connection, name_map: dict[str, str]
) -> None:
    """Concept pages: bioactivity entities + r2 hierarchy + linked counts."""
    entities = pd.read_sql(
        text(
            "SELECT foodatlas_id, common_name, synonyms, external_ids, attributes"
            " FROM base_entities WHERE entity_type = 'bioactivity'"
        ),
        conn,
    )
    if entities.empty:
        return

    parents, children = _hierarchy(conn, set(entities["foodatlas_id"]), name_map)
    n_chemicals = _linked_counts(conn, "r6")
    n_foods = _linked_counts(conn, "r5")

    result_rows = []
    for r in entities.itertuples(index=False):
        attrs = r.attributes if isinstance(r.attributes, dict) else {}
        ext = r.external_ids if isinstance(r.external_ids, dict) else {}
        result_rows.append(
            {
                "foodatlas_id": r.foodatlas_id,
                "common_name": r.common_name,
                "synonyms": list(r.synonyms) if r.synonyms is not None else [],
                "description": attrs.get("description", ""),
                "external_ids": json.dumps(ext),
                "parents": json.dumps(parents.get(r.foodatlas_id, [])),
                "children": json.dumps(children.get(r.foodatlas_id, [])),
                "n_foods": int(n_foods.get(r.foodatlas_id, 0)),
                "n_chemicals": int(n_chemicals.get(r.foodatlas_id, 0)),
            }
        )
    df = pd.DataFrame(result_rows)
    bulk_copy(
        conn,
        "mv_bioactivity_entities",
        df,
        [
            "foodatlas_id",
            "common_name",
            "synonyms",
            "description",
            "external_ids",
            "parents",
            "children",
            "n_foods",
            "n_chemicals",
        ],
    )
    logger.info("Bioactivity entities: %d rows", len(df))


def _hierarchy(
    conn: Connection, bio_ids: set[str], name_map: dict[str, str]
) -> tuple[dict[str, list], dict[str, list]]:
    """From r2 among bioactivity ids: child→parents, parent→children (as {id,name})."""
    r2 = pd.read_sql(
        text("SELECT head_id, tail_id FROM base_triplets WHERE relationship_id = 'r2'"),
        conn,
    )
    r2 = r2[r2["head_id"].isin(bio_ids) & r2["tail_id"].isin(bio_ids)]
    parents: dict[str, list] = defaultdict(list)
    children: dict[str, list] = defaultdict(list)
    # head is_a tail
    for child, parent in zip(r2["head_id"], r2["tail_id"], strict=False):
        parents[child].append(
            {"foodatlas_id": parent, "common_name": name_map.get(parent, parent)}
        )
        children[parent].append(
            {"foodatlas_id": child, "common_name": name_map.get(child, child)}
        )
    return parents, children


def _linked_counts(conn: Connection, rel_id: str) -> dict[str, int]:
    """bioactivity_id (tail) → number of distinct linked head entities."""
    df = pd.read_sql(
        text(
            "SELECT tail_id, count(DISTINCT head_id) AS n FROM base_triplets"
            " WHERE relationship_id = :rel GROUP BY tail_id"
        ),
        conn,
        params={"rel": rel_id},
    )
    return dict(zip(df["tail_id"], df["n"], strict=False))
