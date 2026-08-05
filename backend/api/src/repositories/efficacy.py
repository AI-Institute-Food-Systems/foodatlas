"""Food–chemical efficacy repository — queries mv_food_chemical_efficacy.

Reads only materialized views (never base tables), matching the rest of the
API. One row per (food, chemical, bioactivity): the food's dietary dose of the
chemical placed against the chemical's dose-response curve for that bioactivity.
LEFT JOINs mv_chemical_bioactivity so callers can see how many total assays
back the (chemical, bioactivity) pair — n_curves is a subset (records with a
fittable AC50); n_measurements_total is the full count the modal will render.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_SELECT_COLUMNS = """
    e.food_name, e.food_foodatlas_id,
    e.chemical_name, e.chemical_foodatlas_id, e.cid,
    e.bioactivity_name, e.bioactivity_foodatlas_id, e.bioactivity_id_raw,
    e.food_conc_mg_per_100g, e.food_conc_mass_fraction_pct, e.conc_quality_flag,
    e.molecular_weight, e.food_conc_m, e.food_conc_logm,
    e.rep_source_assay_id, e.endpoint_type, e.endpoint_class, e.curve_method,
    e.logac50, e.hillslope, e.zeroactivity, e.infiniteactivity,
    e.n_curves, e.n_curves_4param, e.curve_agreement, e.ac50_spread_log,
    e.logac50_median, e.logac50_min, e.logac50_max,
    e.dose_over_ac50_log, e.conc_vs_ac50, e.efficacy_fraction, e.efficacy_response,
    e.saturated,
    COALESCE(b.measurement_count, 0) AS n_measurements_total
"""


async def get_food_efficacy(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """A food's chemical×bioactivity efficacy rows (most efficacious first)."""
    result = await session.execute(
        text(f"""
            SELECT {_SELECT_COLUMNS}
            FROM mv_food_chemical_efficacy e
            LEFT JOIN mv_chemical_bioactivity b
              ON e.chemical_foodatlas_id = b.chemical_foodatlas_id
             AND e.bioactivity_foodatlas_id = b.bioactivity_foodatlas_id
            WHERE e.food_name = :name
            ORDER BY e.efficacy_response DESC NULLS LAST
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}
