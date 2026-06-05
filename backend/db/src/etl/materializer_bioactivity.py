"""Build the four bioactivity MVs from base tables.

- mv_bioactivity_entities (r5/r6/r7 participants)
- mv_chemical_bioactivity_measurement (r5)
- mv_food_bioactivity_exhibits (r6, direct + inherited)
- mv_bioactivity_disease_association (r7)
"""

import json
import logging
from collections import defaultdict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from ._bioactivity_helpers import build_measurement, explode_attestations, name_map
from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)


def refresh_bioactivity(conn: Connection) -> None:
    """Build all four bioactivity MVs. Assumes their tables are truncated."""
    logger.info("Building bioactivity entities...")
    materialize_bioactivity_entities(conn)
    logger.info("Building chemical-bioactivity measurements...")
    materialize_chemical_bioactivity_measurement(conn)
    logger.info("Building food-bioactivity exhibits...")
    materialize_food_bioactivity_exhibits(conn)
    logger.info("Building bioactivity-disease associations...")
    materialize_bioactivity_disease_association(conn)


def materialize_bioactivity_entities(conn: Connection) -> None:
    """Populate mv_bioactivity_entities with bioactivities used in r5/r6/r7."""
    entities = pd.read_sql(
        text("SELECT * FROM base_entities WHERE entity_type = 'bioactivity'"),
        conn,
    )
    if entities.empty:
        return

    triplets = pd.read_sql(
        text(
            "SELECT head_id, tail_id, relationship_id FROM base_triplets"
            " WHERE relationship_id IN ('r5','r6','r7')"
        ),
        conn,
    )
    if triplets.empty:
        return

    r5r6_mask = triplets["relationship_id"].isin(["r5", "r6"])
    r5r6_tails = set(triplets[r5r6_mask]["tail_id"])
    r7_heads = set(triplets[triplets["relationship_id"] == "r7"]["head_id"])
    relevant_ids = r5r6_tails | r7_heads

    rows = entities[entities["foodatlas_id"].isin(relevant_ids)].copy()
    rows["description"] = rows["attributes"].apply(
        lambda a: a.get("description", "") if isinstance(a, dict) else ""
    )
    rows["ambiguity_siblings"] = "[]"

    bulk_copy(
        conn,
        "mv_bioactivity_entities",
        rows,
        [
            "foodatlas_id",
            "entity_type",
            "common_name",
            "scientific_name",
            "synonyms",
            "external_ids",
            "description",
            "ambiguity_siblings",
        ],
    )
    logger.info("Bioactivity entities: %d rows", len(rows))


def materialize_chemical_bioactivity_measurement(conn: Connection) -> None:
    """One row per (chemical, bioactivity); measurements aggregates assay rows."""
    triplets = pd.read_sql(
        text(
            "SELECT head_id, tail_id, attestation_ids FROM base_triplets"
            " WHERE relationship_id = 'r5'"
        ),
        conn,
    )
    if triplets.empty:
        return

    atts = pd.read_sql(text("SELECT * FROM base_bioactivity_attestations"), conn)
    names = name_map(conn)

    exploded = explode_attestations(triplets)
    merged = exploded.merge(atts, on="attestation_id", how="inner")

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in merged.to_dict(orient="records"):
        groups[(row["head_id"], row["tail_id"])].append(build_measurement(row))

    out = [
        {
            "chemical_name": names.get(chem_id, chem_id),
            "chemical_foodatlas_id": chem_id,
            "bioactivity_name": names.get(bio_id, bio_id),
            "bioactivity_foodatlas_id": bio_id,
            "measurement_count": len(measurements),
            "measurements": json.dumps(measurements),
        }
        for (chem_id, bio_id), measurements in groups.items()
    ]
    if not out:
        return
    df = pd.DataFrame(out)
    bulk_copy(
        conn,
        "mv_chemical_bioactivity_measurement",
        df,
        [
            "chemical_name",
            "chemical_foodatlas_id",
            "bioactivity_name",
            "bioactivity_foodatlas_id",
            "measurement_count",
            "measurements",
        ],
    )
    logger.info("Chemical-bioactivity measurements: %d rows", len(df))


def materialize_food_bioactivity_exhibits(conn: Connection) -> None:
    """One row per (food, bioactivity, exhibit_type).

    ``efficacy_pred`` is reserved on the schema; computing it requires the
    bridging chemical's molar concentration in the food, which is not yet
    available from the composition MV (it stores mg/100g, conversion needs
    molecular weight). Leaving NULL for v1; backfill in a follow-up when
    molecular weights are wired through.
    """
    triplets = pd.read_sql(
        text(
            "SELECT head_id, tail_id, attestation_ids FROM base_triplets"
            " WHERE relationship_id = 'r6'"
        ),
        conn,
    )
    if triplets.empty:
        return

    atts = pd.read_sql(text("SELECT * FROM base_bioactivity_attestations"), conn)
    names = name_map(conn)

    exploded = explode_attestations(triplets)
    merged = exploded.merge(atts, on="attestation_id", how="inner")
    merged["exhibit_type"] = merged["exhibit_type"].fillna("direct")

    groups: dict[tuple[str, str, str], dict] = {}
    for row in merged.to_dict(orient="records"):
        key = (row["head_id"], row["tail_id"], row["exhibit_type"])
        bucket = groups.setdefault(
            key, {"via_chemical_id": row.get("via_chemical_id"), "evidences": []}
        )
        bucket["evidences"].append(build_measurement(row))
        if bucket["via_chemical_id"] is None and row.get("via_chemical_id"):
            bucket["via_chemical_id"] = row["via_chemical_id"]

    out = []
    for (food_id, bio_id, exhibit_type), bucket in groups.items():
        via_chem_id = bucket["via_chemical_id"]
        out.append(
            {
                "food_name": names.get(food_id, food_id),
                "food_foodatlas_id": food_id,
                "bioactivity_name": names.get(bio_id, bio_id),
                "bioactivity_foodatlas_id": bio_id,
                "exhibit_type": exhibit_type,
                "via_chemical_id": via_chem_id,
                "via_chemical_name": names.get(via_chem_id) if via_chem_id else None,
                "efficacy_pred": None,
                "evidence_count": len(bucket["evidences"]),
                "evidences": json.dumps(bucket["evidences"]),
            }
        )

    if not out:
        return
    df = pd.DataFrame(out)
    bulk_copy(
        conn,
        "mv_food_bioactivity_exhibits",
        df,
        [
            "food_name",
            "food_foodatlas_id",
            "bioactivity_name",
            "bioactivity_foodatlas_id",
            "exhibit_type",
            "via_chemical_id",
            "via_chemical_name",
            "efficacy_pred",
            "evidence_count",
            "evidences",
        ],
    )
    logger.info("Food-bioactivity exhibits: %d rows", len(df))


def materialize_bioactivity_disease_association(conn: Connection) -> None:
    """One row per (bioactivity, disease); target_ids aggregated across BDMs."""
    triplets = pd.read_sql(
        text(
            "SELECT head_id, tail_id, attestation_ids FROM base_triplets"
            " WHERE relationship_id = 'r7'"
        ),
        conn,
    )
    if triplets.empty:
        return

    atts = pd.read_sql(text("SELECT * FROM base_bioactivity_attestations"), conn)
    names = name_map(conn)

    exploded = explode_attestations(triplets)
    merged = exploded.merge(atts, on="attestation_id", how="inner")

    groups: dict[tuple[str, str], dict] = {}
    for row in merged.to_dict(orient="records"):
        key = (row["head_id"], row["tail_id"])
        bucket = groups.setdefault(
            key,
            {"polarity": row.get("polarity"), "targets": set(), "evidences": []},
        )
        for tid in row.get("target_ids") or []:
            bucket["targets"].add(tid)
        bucket["evidences"].append(
            {
                "attestation_id": row["attestation_id"],
                "bioactivity_metadata_id": row["bioactivity_metadata_id"],
                "target_ids": row.get("target_ids") or [],
                "evidence_source": row.get("evidence_source"),
                "evidence_type": row.get("evidence_type"),
            }
        )

    out = [
        {
            "bioactivity_name": names.get(bio_id, bio_id),
            "bioactivity_foodatlas_id": bio_id,
            "disease_name": names.get(dis_id, dis_id),
            "disease_foodatlas_id": dis_id,
            "polarity": bucket["polarity"],
            "target_ids": sorted(bucket["targets"]),
            "evidence_count": len(bucket["evidences"]),
            "evidences": json.dumps(bucket["evidences"]),
        }
        for (bio_id, dis_id), bucket in groups.items()
    ]
    if not out:
        return
    df = pd.DataFrame(out)
    bulk_copy(
        conn,
        "mv_bioactivity_disease_association",
        df,
        [
            "bioactivity_name",
            "bioactivity_foodatlas_id",
            "disease_name",
            "disease_foodatlas_id",
            "polarity",
            "target_ids",
            "evidence_count",
            "evidences",
        ],
    )
    logger.info("Bioactivity-disease associations: %d rows", len(df))
