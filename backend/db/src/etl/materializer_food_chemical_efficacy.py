"""Build mv_food_chemical_efficacy from base_food_chemical_efficacy.

Resolves each fact row's chemical (by PubChem ``cid``) and bioactivity concept
(by ``E300…`` native id) to their entity names/ids; the food is already an
entity id. Rows whose food or chemical cannot be resolved are dropped (logged);
``UNCLASSIFIED`` bioactivity rows are kept with empty bioactivity fields.
"""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy

logger = logging.getLogger(__name__)

_METRIC_COLUMNS = [
    "food_conc_mg_per_100g",
    "food_conc_mass_fraction_pct",
    "conc_quality_flag",
    "molecular_weight",
    "food_conc_m",
    "food_conc_logm",
    "rep_source_assay_id",
    "endpoint_type",
    "endpoint_class",
    "curve_method",
    "logac50",
    "hillslope",
    "zeroactivity",
    "infiniteactivity",
    "n_curves",
    "n_curves_4param",
    "curve_agreement",
    "ac50_spread_log",
    "logac50_median",
    "logac50_min",
    "logac50_max",
    "dose_over_ac50_log",
    "conc_vs_ac50",
    "efficacy_fraction",
    "efficacy_response",
    "saturated",
]

_MV_COLUMNS = [
    "food_name",
    "food_foodatlas_id",
    "chemical_name",
    "chemical_foodatlas_id",
    "cid",
    "bioactivity_name",
    "bioactivity_foodatlas_id",
    "bioactivity_id_raw",
    *_METRIC_COLUMNS,
]


def materialize_food_chemical_efficacy(conn: Connection) -> None:
    """Resolve the efficacy fact table to entity names and write the mv."""
    efficacy = pd.read_sql(text("SELECT * FROM base_food_chemical_efficacy"), conn)
    if efficacy.empty:
        logger.info("No food-chemical efficacy to materialize (skipping).")
        return

    entities = pd.read_sql(
        text(
            "SELECT foodatlas_id, entity_type, common_name, external_ids"
            " FROM base_entities"
        ),
        conn,
    )
    name_map = entities.set_index("foodatlas_id")["common_name"].to_dict()
    cid_to_chem = _external_id_map(entities, "chemical", "pubchem_compound")
    native_to_bio = _external_id_map(entities, "bioactivity", "bioactivity_concept")

    resolved = _resolve(efficacy, name_map, cid_to_chem, native_to_bio)
    bulk_copy(conn, "mv_food_chemical_efficacy", resolved, _MV_COLUMNS)
    logger.info(
        "Food-chemical efficacy: %d/%d rows materialized.",
        len(resolved),
        len(efficacy),
    )


def _external_id_map(
    entities: pd.DataFrame, entity_type: str, key: str
) -> dict[str, str]:
    """Map each external id under ``key`` to its foodatlas_id, for one type."""
    out: dict[str, str] = {}
    subset = entities[entities["entity_type"] == entity_type]
    for foodatlas_id, external_ids in zip(
        subset["foodatlas_id"], subset["external_ids"], strict=False
    ):
        if not isinstance(external_ids, dict):
            continue
        for value in external_ids.get(key, []) or []:
            out.setdefault(_norm_cid(value), foodatlas_id)
    return out


def _resolve(
    efficacy: pd.DataFrame,
    name_map: dict[str, str],
    cid_to_chem: dict[str, str],
    native_to_bio: dict[str, str],
) -> pd.DataFrame:
    """Attach resolved food/chemical/bioactivity names and drop unresolved rows."""
    df = efficacy.copy()
    df["food_foodatlas_id"] = df["foodatlas_id"]
    df["food_name"] = df["foodatlas_id"].map(name_map)
    df["chemical_foodatlas_id"] = df["cid"].map(
        lambda c: cid_to_chem.get(_norm_cid(c), "")
    )
    df["chemical_name"] = df["chemical_foodatlas_id"].map(lambda i: name_map.get(i, ""))
    df["bioactivity_id_raw"] = df["bioactivity_id"]
    df["bioactivity_foodatlas_id"] = df["bioactivity_id"].map(
        lambda n: native_to_bio.get(str(n), "")
    )
    df["bioactivity_name"] = df["bioactivity_foodatlas_id"].map(
        lambda i: name_map.get(i, "")
    )

    keep = df["food_name"].notna() & (df["chemical_foodatlas_id"] != "")
    dropped = int((~keep).sum())
    if dropped:
        logger.warning(
            "Dropped %d efficacy rows with unresolved food/chemical.", dropped
        )
    return df[keep]


def _norm_cid(value: object) -> str:
    """Normalize a CID to a plain integer string (``5281224.0`` → ``5281224``)."""
    return str(value).split(".")[0]
