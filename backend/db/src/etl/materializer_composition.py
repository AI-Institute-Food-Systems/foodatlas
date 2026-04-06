"""Build mv_food_chemical_composition (vectorized)."""

import json
import logging

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)
PMC_URL = "https://www.ncbi.nlm.nih.gov/pmc/?term="


def materialize_food_chemical_composition(conn: Connection) -> None:
    """Build mv_food_chemical_composition from r1 triplets + attestations.

    Vectorized: explodes attestation_ids, joins attestations + evidence
    in bulk, then groups by (head_id, tail_id) to build evidence JSON.
    """
    r1 = pd.read_sql(
        text(
            "SELECT head_id, tail_id, attestation_ids"
            " FROM base_triplets WHERE relationship_id = 'r1'"
        ),
        conn,
    )
    attestations = pd.read_sql(text("SELECT * FROM base_attestations"), conn)
    evidence = pd.read_sql(text("SELECT * FROM base_evidence"), conn)
    entities = pd.read_sql(
        text("SELECT foodatlas_id, common_name FROM base_entities"), conn
    )
    chem_class = pd.read_sql(
        text("SELECT foodatlas_id, nutrient_classification FROM mv_chemical_entities"),
        conn,
    )

    name_map = entities.set_index("foodatlas_id")["common_name"].to_dict()
    nutr_map = chem_class.set_index("foodatlas_id")["nutrient_classification"].to_dict()

    # Explode so each (triplet, att_id) is one row, then join.
    r1_ex = r1.explode("attestation_ids").rename(
        columns={"attestation_ids": "attestation_id"}
    )
    r1_ex = r1_ex.dropna(subset=["attestation_id"])
    merged = r1_ex.merge(attestations, on="attestation_id", how="inner")
    merged = merged.merge(evidence, on="evidence_id", how="left", suffixes=("", "_ev"))

    # Filter zero concentrations.
    merged["conc_value"] = pd.to_numeric(merged["conc_value"], errors="coerce")
    merged = merged[~((merged["conc_value"].notna()) & (merged["conc_value"] == 0))]

    # Map entity names + build display names.
    merged["food_name"] = merged["head_id"].map(name_map)
    merged["chem_name"] = merged["tail_id"].map(name_map)
    is_db = merged["source"].isin(["fdc", "dmd"])
    merged["show_food"] = merged["food_name"].where(
        is_db,
        merged["head_name_raw"].where(
            merged["head_name_raw"] != "", merged["food_name"]
        ),
    )
    merged["show_chem"] = merged["chem_name"].where(
        is_db,
        merged["tail_name_raw"].where(
            merged["tail_name_raw"] != "", merged["chem_name"]
        ),
    )

    # Group by triplet and build evidence JSON.
    result_rows = []
    for (head_id, tail_id), group in merged.groupby(["head_id", "tail_id"]):
        ev = _build_evidence_json(group)
        if not any(ev.values()):
            continue
        all_ev = (ev["fdc"] or []) + (ev["foodatlas"] or []) + (ev["dmd"] or [])
        median_conc = _compute_median(all_ev)
        result_rows.append(
            {
                "food_name": name_map.get(head_id, ""),
                "food_foodatlas_id": head_id,
                "chemical_name": name_map.get(tail_id, ""),
                "chemical_foodatlas_id": tail_id,
                "nutrient_classification": nutr_map.get(tail_id, []),
                "median_concentration": json.dumps(median_conc)
                if median_conc
                else None,
                "fdc_evidences": json.dumps(ev["fdc"]) if ev["fdc"] else None,
                "foodatlas_evidences": json.dumps(ev["foodatlas"])
                if ev["foodatlas"]
                else None,
                "dmd_evidences": json.dumps(ev["dmd"]) if ev["dmd"] else None,
            }
        )

    if not result_rows:
        return
    result = pd.DataFrame(result_rows)
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


def _build_evidence_json(group: pd.DataFrame) -> dict[str, list | None]:
    """Build {fdc, foodatlas, dmd} evidence lists from a grouped DataFrame."""
    fdc: list[dict] = []
    foodatlas: list[dict] = []
    dmd: list[dict] = []

    for _, row in group.iterrows():
        source = row["source"]
        ref = row["reference"] if isinstance(row["reference"], dict) else {}
        conc_val = row["conc_value"] if pd.notna(row["conc_value"]) else None

        extraction = {
            "extracted_food_name": row["show_food"],
            "extracted_chemical_name": row["show_chem"],
            "extracted_concentration": row["conc_value_raw"] or None,
            "converted_concentration": {
                "value": conc_val,
                "unit": row["conc_unit"] or "",
            },
            "method": source,
        }

        if source in ("fdc", "dmd"):
            bucket = fdc if source == "fdc" else dmd
            upper = source.upper()
            key = "fdc_id" if source == "fdc" else "dmd_concentration_id"
            ref_id = ref.get(key, ref.get("url", ""))
            bucket.append(
                {
                    "premise": None,
                    "reference": {
                        "id": str(ref_id),
                        "url": ref.get("url", ""),
                        "source_name": upper,
                        "display_name": f"{upper} ID",
                    },
                    "extraction": [extraction],
                }
            )
        else:
            _add_foodatlas_evidence(foodatlas, ref, extraction)

    return {"fdc": fdc or None, "foodatlas": foodatlas or None, "dmd": dmd or None}


def _add_foodatlas_evidence(evidences: list, ref: dict, extraction: dict) -> None:
    """Add a FoodAtlas evidence, grouping by premise."""
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


def _compute_median(evidences: list) -> dict | None:
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
