"""Disease↔bioactivity repository — reads ``mv_disease_bioactivity``.

Answers "what do this disease's assay evidence actually measure, and which
chemicals carry it?" — and the same question from the other side.

Three grains off one view:

* :func:`get_disease_bioactivities` — one row per bioactivity, for the tab's
  summary/filter chips.
* :func:`get_disease_bioactivity_chemicals` — one row per (bioactivity,
  chemical), carrying the full per-pair evidence.
* :func:`get_bioactivity_diseases` — the mirror image, one row per disease, for
  the Diseases tab on bioactivity pages.

The two aggregate grains report the bridge's ``therapeutic`` /
``marker/mechanism`` split as *counts of chemicals* rather than as chips. A
chip answers "does this row involve therapeutic evidence?", which at a grain
that rolls up hundreds of chemicals is nearly always "yes, some" and therefore
says nothing. The counts say how much of each.

Deliberately does **not** join ``mv_food_chemical_efficacy``. An earlier
revision attached, per chemical, the food whose dietary dose sat furthest above
that chemical's AC50. The arithmetic was right but the presentation overclaimed:
the AC50 is constant across foods within a (chemical, bioactivity) pair, so the
"best" food was only ever the most concentrated one, and the runners-up were
usually within noise. Layered on the density-1 concentration proxy and no
bioavailability model, a single confident food name implied a precision the
inputs don't support. Left out until the underlying numbers earn it.

Distinct from ``/disease/chemical-associations``, which collapses the assay's
bioactivity away and answers only *which* chemicals are linked.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .target_labels import attach_targets

# Chemicals split by the direction the bridge assigned. A chemical with both
# classes counts once in each — the classes are not mutually exclusive, and
# forcing a winner would misreport the evidence.
_DIRECTION_COUNTS = """
    COUNT(DISTINCT chemical_foodatlas_id)
      FILTER (WHERE 'therapeutic' = ANY(relationships))      AS n_therapeutic,
    COUNT(DISTINCT chemical_foodatlas_id)
      FILTER (WHERE 'marker/mechanism' = ANY(relationships)) AS n_marker,
    COUNT(DISTINCT chemical_foodatlas_id)
      FILTER (WHERE cardinality(literature_directions) > 0)  AS n_literature
"""

# The targets most chemicals share, not every target any chemical touched.
# A union across hundreds of chemicals is a laundry list; the top few by
# chemical count are the proteins the group actually converges on.
#
# Over-fetched because a protein typically appears under both its Entrez and
# UniProt id, and those collapse into one entry only once labels are known —
# see ``target_labels._dedupe``.
_TOP_TARGETS = 12


async def get_disease_bioactivities(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Bioactivity profile of a disease, most chemicals first."""
    result = await session.execute(
        text(f"""
            SELECT bioactivity_name, bioactivity_foodatlas_id,
                   COUNT(DISTINCT chemical_foodatlas_id) AS n_chemicals,
                   SUM(n_assays) AS n_assays,
                   SUM(n_active_measurements) AS n_active_measurements,
                   {_DIRECTION_COUNTS}
            FROM mv_disease_bioactivity
            WHERE disease_name = :name
            GROUP BY bioactivity_name, bioactivity_foodatlas_id
            ORDER BY n_chemicals DESC, n_assays DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_bioactivity_diseases(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Diseases whose bridging assays measure this bioactivity.

    Mirror of :func:`get_disease_bioactivities`. Flat rather than two-level:
    a bioactivity reaches at most 1,282 diseases, few enough to list directly
    without the chip-and-drilldown shape the disease side needs.
    """
    result = await session.execute(
        text(f"""
            WITH scoped AS (
                SELECT * FROM mv_disease_bioactivity WHERE bioactivity_name = :name
            ),
            shared_targets AS (
                SELECT disease_foodatlas_id,
                       (array_agg(gene ORDER BY n_chemicals DESC, gene)
                        )[1:{_TOP_TARGETS}] AS target_genes
                FROM (
                    SELECT disease_foodatlas_id, gene,
                           COUNT(DISTINCT chemical_foodatlas_id) AS n_chemicals
                    FROM scoped, LATERAL unnest(target_genes) AS gene
                    GROUP BY disease_foodatlas_id, gene
                ) ranked
                GROUP BY disease_foodatlas_id
            )
            SELECT s.disease_name, s.disease_foodatlas_id,
                   COUNT(DISTINCT s.chemical_foodatlas_id) AS n_chemicals,
                   SUM(s.n_assays) AS n_assays,
                   SUM(s.n_active_measurements) AS n_active_measurements,
                   {_DIRECTION_COUNTS},
                   COALESCE(MAX(t.target_genes), '{{}}') AS target_genes
            FROM scoped s
            LEFT JOIN shared_targets t USING (disease_foodatlas_id)
            GROUP BY s.disease_name, s.disease_foodatlas_id
            ORDER BY n_chemicals DESC, n_assays DESC, s.disease_name
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    await attach_targets(session, data)
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
                   n_assays, n_active_measurements, relationships,
                   target_genes, assays, literature_directions
            FROM mv_disease_bioactivity
            WHERE disease_name = :name {clause}
            ORDER BY n_assays DESC, n_active_measurements DESC, chemical_name
        """),
        params,
    )
    data = [dict(row._mapping) for row in result]
    await attach_targets(session, data)
    return {"data": data, "metadata": {"row_count": len(data)}}
