"""Disease↔bioactivity repository — reads ``mv_disease_bioactivity``.

Answers "what does this disease's assay evidence actually measure, and which
food chemicals get closest to an active dose?"

Two grains off one view:

* :func:`get_disease_bioactivities` — one row per bioactivity, for the tab's
  summary/filter chips.
* :func:`get_disease_bioactivity_chemicals` — one row per (bioactivity,
  chemical), left-joined to the food-efficacy view so each chemical carries the
  food where it comes closest to its own AC50.

Distinct from ``/disease/chemical-associations``, which collapses the assay's
bioactivity away and answers only *which* chemicals are linked.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Pick one representative food per (chemical, bioactivity): the one whose
# dietary dose sits furthest above the curve's AC50.
#
# Two deliberate choices in the ORDER BY:
#   - `ok` concentrations win ties over `suspect_high` ones. Without this the
#     top of every list is implausible source values (a 9.2%-by-mass "oleic
#     acid in apricot"), which reads as a ranking of data errors.
#   - dose_over_ac50_log, not efficacy_fraction. The Hill fraction saturates —
#     most rows read exactly 100% — so it cannot rank. The log margin can.
_BEST_FOOD_CTE = """
    SELECT DISTINCT ON (chemical_foodatlas_id, bioactivity_foodatlas_id)
           chemical_foodatlas_id, bioactivity_foodatlas_id,
           food_name, food_foodatlas_id,
           food_conc_mg_per_100g, conc_quality_flag,
           efficacy_fraction, dose_over_ac50_log, conc_vs_ac50,
           logac50, n_curves, endpoint_type, saturated
    FROM mv_food_chemical_efficacy
    WHERE efficacy_fraction IS NOT NULL AND bioactivity_foodatlas_id <> ''
    ORDER BY chemical_foodatlas_id, bioactivity_foodatlas_id,
             (conc_quality_flag = 'ok') DESC,
             dose_over_ac50_log DESC NULLS LAST
"""

# Rows backed by a food dose sort above rows that are assay-only, so the
# actionable part of the list is at the top rather than buried under
# pharmaceuticals that never occur in food.
_CHEMICAL_ORDER = """
    ORDER BY (b.dose_over_ac50_log IS NOT NULL) DESC,
             (b.conc_quality_flag = 'ok') DESC,
             b.dose_over_ac50_log DESC NULLS LAST,
             d.n_assays DESC, d.chemical_name
"""


async def get_disease_bioactivities(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Bioactivity profile of a disease, most-evidenced first.

    ``n_dietary_chemicals`` counts the chemicals that actually occur in a food
    with a fittable curve — usually a small fraction of ``n_chemicals``, and
    the honest denominator for anything the UI calls "in food".
    """
    result = await session.execute(
        text(f"""
            WITH best AS ({_BEST_FOOD_CTE})
            SELECT d.bioactivity_name, d.bioactivity_foodatlas_id,
                   COUNT(DISTINCT d.chemical_foodatlas_id) AS n_chemicals,
                   COUNT(DISTINCT d.chemical_foodatlas_id) FILTER (
                       WHERE b.dose_over_ac50_log IS NOT NULL
                   ) AS n_dietary_chemicals,
                   SUM(d.n_assays) AS n_assays,
                   SUM(d.n_active_measurements) AS n_active_measurements,
                   MAX(b.dose_over_ac50_log) AS best_dose_over_ac50_log
            FROM mv_disease_bioactivity d
            LEFT JOIN best b
              ON b.chemical_foodatlas_id = d.chemical_foodatlas_id
             AND b.bioactivity_foodatlas_id = d.bioactivity_foodatlas_id
            WHERE d.disease_name = :name
            GROUP BY d.bioactivity_name, d.bioactivity_foodatlas_id
            ORDER BY n_chemicals DESC, n_assays DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_disease_bioactivity_chemicals(
    session: AsyncSession, common_name: str, bioactivity: str | None = None
) -> dict[str, object]:
    """Chemicals behind a disease's bioactivities, best dietary dose first."""
    clause = "AND d.bioactivity_name = :bioactivity" if bioactivity else ""
    params: dict[str, object] = {"name": common_name}
    if bioactivity:
        params["bioactivity"] = bioactivity

    result = await session.execute(
        text(f"""
            WITH best AS ({_BEST_FOOD_CTE})
            SELECT d.bioactivity_name, d.bioactivity_foodatlas_id,
                   d.chemical_name, d.chemical_foodatlas_id,
                   d.n_assays, d.n_active_measurements, d.relationships,
                   b.food_name, b.food_foodatlas_id,
                   b.food_conc_mg_per_100g, b.conc_quality_flag,
                   b.efficacy_fraction, b.dose_over_ac50_log, b.conc_vs_ac50,
                   b.logac50, b.n_curves, b.endpoint_type, b.saturated
            FROM mv_disease_bioactivity d
            LEFT JOIN best b
              ON b.chemical_foodatlas_id = d.chemical_foodatlas_id
             AND b.bioactivity_foodatlas_id = d.bioactivity_foodatlas_id
            WHERE d.disease_name = :name {clause}
            {_CHEMICAL_ORDER}
        """),
        params,
    )
    data = [_shape_chemical_row(dict(row._mapping)) for row in result]
    n_dietary = sum(1 for row in data if row["dietary"] is not None)
    return {
        "data": data,
        "metadata": {"row_count": len(data), "n_dietary": n_dietary},
    }


# Only ~3% of rows have a food dose behind them; the rest are assay-only
# (mostly pharmaceuticals). Nesting the food fields keeps eleven null keys off
# every one of those rows — on the largest disease that halves the response.
_DIETARY_FIELDS = (
    "food_name",
    "food_foodatlas_id",
    "food_conc_mg_per_100g",
    "conc_quality_flag",
    "efficacy_fraction",
    "dose_over_ac50_log",
    "conc_vs_ac50",
    "logac50",
    "n_curves",
    "endpoint_type",
    "saturated",
)


def _shape_chemical_row(row: dict) -> dict:
    """Move the food-efficacy columns into a nullable ``dietary`` object."""
    dietary = {field: row.pop(field) for field in _DIETARY_FIELDS}
    row["dietary"] = dietary if dietary["dose_over_ac50_log"] is not None else None
    return row
