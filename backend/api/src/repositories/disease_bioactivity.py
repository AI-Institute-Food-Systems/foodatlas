"""Disease↔bioactivity repository — reads ``mv_disease_bioactivity``.

Answers "what do this disease's assay evidence actually measure, and which
chemicals carry it?"

Two grains off one view:

* :func:`get_disease_bioactivities` — one row per bioactivity, for the tab's
  summary/filter chips.
* :func:`get_disease_bioactivity_chemicals` — one row per (bioactivity,
  chemical).

Deliberately does **not** join ``mv_food_chemical_efficacy``. An earlier
revision attached, per chemical, the food whose dietary dose sat furthest above
that chemical's AC50. The arithmetic was right but the presentation overclaimed:
the AC50 is constant across foods within a (chemical, bioactivity) pair, so the
"best" food was only ever the most concentrated one, and the runners-up were
usually within noise of it. Layered on the density-1 concentration proxy and no
bioavailability model, a single confident food name implied a precision the
inputs don't support. Left out until the underlying numbers earn it.

Distinct from ``/disease/chemical-associations``, which collapses the assay's
bioactivity away and answers only *which* chemicals are linked.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def get_disease_bioactivities(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Bioactivity profile of a disease, most chemicals first."""
    result = await session.execute(
        text("""
            SELECT bioactivity_name, bioactivity_foodatlas_id,
                   COUNT(DISTINCT chemical_foodatlas_id) AS n_chemicals,
                   SUM(n_assays) AS n_assays,
                   SUM(n_active_measurements) AS n_active_measurements
            FROM mv_disease_bioactivity
            WHERE disease_name = :name
            GROUP BY bioactivity_name, bioactivity_foodatlas_id
            ORDER BY n_chemicals DESC, n_assays DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_disease_bioactivity_chemicals(
    session: AsyncSession, common_name: str, bioactivity: str | None = None
) -> dict[str, object]:
    """Chemicals behind a disease's bioactivities, best-evidenced first.

    Ordered by bridging assay count — the amount of evidence standing behind
    the link, which is the one thing this view can rank honestly.
    """
    clause = "AND bioactivity_name = :bioactivity" if bioactivity else ""
    params: dict[str, object] = {"name": common_name}
    if bioactivity:
        params["bioactivity"] = bioactivity

    result = await session.execute(
        text(f"""
            SELECT bioactivity_name, bioactivity_foodatlas_id,
                   chemical_name, chemical_foodatlas_id,
                   n_assays, n_active_measurements, relationships
            FROM mv_disease_bioactivity
            WHERE disease_name = :name {clause}
            ORDER BY n_assays DESC, n_active_measurements DESC, chemical_name
        """),
        params,
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}
