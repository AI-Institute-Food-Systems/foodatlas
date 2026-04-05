"""Materialize denormalized API tables from base tables."""

import json
import logging

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy, truncate_tables
from .materializer_correlation import materialize_chemical_disease_correlation

logger = logging.getLogger(__name__)
PMC_URL = "https://www.ncbi.nlm.nih.gov/pmc/?term="
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
    _materialize_entity_views(conn)
    _materialize_food_chemical_composition(conn)
    materialize_chemical_disease_correlation(conn)
    conn.commit()


def _materialize_entity_views(conn: Connection) -> None:
    """Compute mv_food_entities, mv_chemical_entities, mv_disease_entities."""
    entities = pd.read_sql(text("SELECT * FROM base_entities"), conn)
    triplets = pd.read_sql(text("SELECT * FROM base_triplets"), conn)

    r1 = triplets[triplets["relationship_id"] == "r1"]
    r2 = triplets[triplets["relationship_id"] == "r2"]
    r3r4 = triplets[triplets["relationship_id"].isin(["r3", "r4"])]

    entity_map = entities.set_index("foodatlas_id")

    # Food entities: foods that appear as head in r1 triplets
    food_ids = set(r1["head_id"])
    foods = entities[
        (entities["entity_type"] == "food") & (entities["foodatlas_id"].isin(food_ids))
    ].copy()
    foods["food_classification"] = foods["foodatlas_id"].apply(
        lambda fid: _get_classifications(fid, r2, entity_map, "foodon")
    )
    _insert_mv_entities(conn, "mv_food_entities", foods, ["food_classification"])

    # Chemical entities: chemicals that appear as tail in r1 triplets
    chem_ids = set(r1["tail_id"])
    chemicals = entities[
        (entities["entity_type"] == "chemical")
        & (entities["foodatlas_id"].isin(chem_ids))
    ].copy()
    chemicals["chemical_classification"] = chemicals["foodatlas_id"].apply(
        lambda fid: _get_classifications(fid, r2, entity_map, "chebi")
    )
    chemicals["nutrient_classification"] = chemicals["foodatlas_id"].apply(
        lambda fid: _get_classifications(fid, r2, entity_map, "cdno")
    )
    _insert_mv_entities(
        conn,
        "mv_chemical_entities",
        chemicals,
        ["chemical_classification", "nutrient_classification"],
    )

    # Disease entities: diseases in r3/r4 triplets (tail_id)
    # Only diseases whose correlated chemicals are also in food composition
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


def _get_classifications(
    entity_id: str,
    r2_triplets: pd.DataFrame,
    entity_map: pd.DataFrame,
    source_prefix: str,
) -> list[str]:
    """Get classification labels from IS_A triplets by source prefix."""
    matches = r2_triplets[
        (r2_triplets["head_id"] == entity_id)
        & (r2_triplets["source"].str.contains(source_prefix, case=False, na=False))
    ]
    return [
        entity_map.loc[tid, "common_name"]
        for tid in matches["tail_id"]
        if tid in entity_map.index
    ]


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


def _materialize_food_chemical_composition(conn: Connection) -> None:
    """Build mv_food_chemical_composition from r1 triplets + attestations."""
    r1_triplets = pd.read_sql(
        text("SELECT * FROM base_triplets WHERE relationship_id = 'r1'"), conn
    )
    attestations = pd.read_sql(text("SELECT * FROM base_attestations"), conn)
    evidence = pd.read_sql(text("SELECT * FROM base_evidence"), conn)
    entities = pd.read_sql(
        text("SELECT foodatlas_id, common_name FROM base_entities"), conn
    )
    # Get nutrient classification from mv_chemical_entities
    chem_class = pd.read_sql(
        text("SELECT foodatlas_id, nutrient_classification FROM mv_chemical_entities"),
        conn,
    )

    name_map = entities.set_index("foodatlas_id")["common_name"].to_dict()
    att_map = attestations.set_index("attestation_id")
    ev_map = evidence.set_index("evidence_id")
    nutr_map = chem_class.set_index("foodatlas_id")["nutrient_classification"].to_dict()

    rows = []
    for _, triplet in r1_triplets.iterrows():
        row = _build_composition_row(triplet, name_map, att_map, ev_map, nutr_map)
        if row is not None:
            rows.append(row)

    if not rows:
        return

    result = pd.DataFrame(rows)
    columns = [
        "food_name",
        "food_foodatlas_id",
        "chemical_name",
        "chemical_foodatlas_id",
        "nutrient_classification",
        "median_concentration",
        "fdc_evidences",
        "foodatlas_evidences",
        "dmd_evidences",
    ]
    bulk_copy(conn, "mv_food_chemical_composition", result, columns)
    logger.info("Food-chemical composition: %d rows", len(result))


def _build_composition_row(
    triplet: pd.Series,
    name_map: dict[str, str],
    att_map: pd.DataFrame,
    ev_map: pd.DataFrame,
    nutr_map: dict[str, list[str]],
) -> dict | None:
    """Build one mv_food_chemical_composition row from a triplet."""
    food_id = triplet["head_id"]
    chem_id = triplet["tail_id"]
    att_ids = triplet["attestation_ids"] or []

    food_name = name_map.get(food_id, "")
    chem_name = name_map.get(chem_id, "")

    fdc_ev, fa_ev, dmd_ev = _group_evidences(
        att_ids, att_map, ev_map, food_name, chem_name
    )
    if not fdc_ev and not fa_ev and not dmd_ev:
        return None

    all_ev = (fdc_ev or []) + (fa_ev or []) + (dmd_ev or [])
    median_conc = _compute_median_concentration(all_ev)

    return {
        "food_name": food_name,
        "food_foodatlas_id": food_id,
        "chemical_name": chem_name,
        "chemical_foodatlas_id": chem_id,
        "nutrient_classification": nutr_map.get(chem_id, []),
        "median_concentration": json.dumps(median_conc) if median_conc else None,
        "fdc_evidences": json.dumps(fdc_ev) if fdc_ev else None,
        "foodatlas_evidences": json.dumps(fa_ev) if fa_ev else None,
        "dmd_evidences": json.dumps(dmd_ev) if dmd_ev else None,
    }


def _group_evidences(
    att_ids: list[str],
    att_map: pd.DataFrame,
    ev_map: pd.DataFrame,
    food_name: str,
    chem_name: str,
) -> tuple[list | None, list | None, list | None]:
    """Group attestations into fdc/foodatlas/dmd evidence lists."""
    fdc: list[dict] = []
    foodatlas: list[dict] = []
    dmd: list[dict] = []

    for att_id in att_ids:
        if att_id not in att_map.index:
            continue
        att = att_map.loc[att_id]
        ev_id = att["evidence_id"]
        ev = ev_map.loc[ev_id] if ev_id in ev_map.index else None
        ref = ev["reference"] if ev is not None else {}
        source = att["source"]

        conc_val = att["conc_value"]
        if conc_val is not None and float(conc_val) == 0:
            continue

        extraction = {
            "extracted_food_name": att["head_name_raw"] or food_name,
            "extracted_chemical_name": att["tail_name_raw"] or chem_name,
            "extracted_concentration": att["conc_value_raw"] or None,
            "converted_concentration": {
                "value": conc_val,
                "unit": att["conc_unit"] or "",
            },
            "method": source,
        }

        if source in ("fdc", "dmd"):
            bucket = fdc if source == "fdc" else dmd
            evidence_obj = _build_db_evidence(source, ref, extraction)
            bucket.append(evidence_obj)
        else:
            _add_foodatlas_evidence(foodatlas, ref, extraction)

    return (fdc or None, foodatlas or None, dmd or None)


def _build_db_evidence(source: str, ref: dict, extraction: dict) -> dict:
    """Build a FoodEvidence object for FDC/DMD sources."""
    source_upper = source.upper()
    ref_id = ref.get("dmd_concentration_id", ref.get("url", ""))
    if source == "fdc":
        ref_id = ref.get("fdc_id", ref.get("url", ""))
    return {
        "premise": None,
        "reference": {
            "id": str(ref_id),
            "url": ref.get("url", ""),
            "source_name": source_upper,
            "display_name": f"{source_upper} ID",
        },
        "extraction": [extraction],
    }


def _add_foodatlas_evidence(evidences: list, ref: dict, extraction: dict) -> None:
    """Add a FoodAtlas (lit2kg) evidence, grouping by premise."""
    pmcid = ref.get("pmcid", "")
    premise = ref.get("text", "")

    for existing in evidences:
        if existing["premise"] == premise:
            existing["extraction"].append(extraction)
            return

    evidences.append(
        {
            "premise": premise,
            "reference": {
                "id": str(pmcid),
                "url": f"{PMC_URL}{pmcid}" if pmcid else "",
                "source_name": "FoodAtlas",
                "display_name": "PMC ID",
            },
            "extraction": [extraction],
        }
    )


def _compute_median_concentration(evidences: list) -> dict | None:
    """Compute median concentration across all evidence extractions."""
    values = []
    for ev in evidences:
        for ext in ev.get("extraction", []):
            conc = ext.get("converted_concentration", {})
            val = conc.get("value")
            unit = conc.get("unit", "")
            if val is not None and float(val) != 0 and unit == "mg/100g":
                values.append(float(val))
    if not values:
        return None
    median = float(np.median(values))
    formatted = f"{median:.6f}".rstrip("0").rstrip(".")
    return {"unit": "mg/100g", "value": formatted}
