"""Food–chemical efficacy repository — queries mv_food_chemical_efficacy.

Reads only the materialized view (never base tables), matching the rest of the
API. One row per (food, chemical, bioactivity): the food's dietary dose of the
chemical placed against the chemical's dose-response curve for that bioactivity.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_SELECT_COLUMNS = """
    food_name, food_foodatlas_id,
    chemical_name, chemical_foodatlas_id, cid,
    bioactivity_name, bioactivity_foodatlas_id, bioactivity_id_raw,
    food_conc_mg_per_100g, food_conc_mass_fraction_pct, conc_quality_flag,
    molecular_weight, food_conc_m, food_conc_logm,
    rep_source_assay_id, endpoint_type, endpoint_class, curve_method,
    logac50, hillslope, zeroactivity, infiniteactivity,
    n_curves, n_curves_4param, curve_agreement, ac50_spread_log,
    logac50_median, logac50_min, logac50_max,
    dose_over_ac50_log, conc_vs_ac50, efficacy_fraction, efficacy_response,
    saturated
"""


async def get_food_efficacy(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """A food's chemical×bioactivity efficacy rows (most efficacious first)."""
    result = await session.execute(
        text(f"""
            SELECT {_SELECT_COLUMNS}
            FROM mv_food_chemical_efficacy WHERE food_name = :name
            ORDER BY efficacy_response DESC NULLS LAST
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}
