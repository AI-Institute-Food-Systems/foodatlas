"""Chemical↔disease (bioactivity-inferred) repository.

Reads only ``mv_chemical_disease_bioactivity`` — associations inferred from a
chemical being *active* in assays the bioactivity-disease bridge ties to a
disease. Distinct from ``/chemical/correlation`` (CTD literature correlations).

Each row carries three kinds of evidence beyond the raw counts:

* ``relationships`` — the CTD direct-evidence class(es) the bridge assigned:
  ``therapeutic`` (the chemical mitigates the disease) or ``marker/mechanism``
  (it marks or drives it). Opposite directions, so the UI must not flatten
  them into a single "associated with".
* ``target_genes`` / ``target_labels`` — the assay's protein target, i.e. what
  the link actually runs through. Ids come from the view; readable names are
  joined in from ``mv_assay_target_labels``.
* ``literature_directions`` — the same two-value vocabulary, but sourced from
  CTD literature rather than the assay bridge, so the two can be compared.
"""

import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_COLUMNS = """
    m.chemical_name, m.chemical_foodatlas_id, m.disease_name,
    m.disease_foodatlas_id, m.n_assays, m.n_active_measurements,
    m.relationships, m.target_genes, m.assays, m.literature_directions
"""

# Labels are a display nicety: a lateral subquery keeps them aligned with
# target_genes without exploding the row, and an unlabelled gene simply falls
# back to its id in the UI.
_LABELS = """
    COALESCE((
        SELECT array_agg(l.label ORDER BY l.label)
        FROM mv_assay_target_labels l
        WHERE l.gene_id = ANY(m.target_genes)
    ), '{}') AS target_labels
"""

_ORDER = "ORDER BY m.n_assays DESC, m.n_active_measurements DESC"


async def get_chemical_disease_associations(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Diseases associated with a chemical (most shared assays first)."""
    return await _query(session, "m.chemical_name", common_name)


async def get_disease_chemical_associations(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Chemicals associated with a disease (most shared assays first)."""
    return await _query(session, "m.disease_name", common_name)


async def _query(
    session: AsyncSession, filter_column: str, common_name: str
) -> dict[str, object]:
    """Run one direction of the association query, labels best-effort.

    ``mv_assay_target_labels`` is a later addition, so an environment that has
    not re-run the ETL still gets its rows — just without readable target
    names. Losing a label is not a reason to lose the association.
    """
    sql = f"""
        SELECT {_COLUMNS}, {_LABELS}
        FROM mv_chemical_disease_bioactivity m
        WHERE {filter_column} = :name
        {_ORDER}
    """
    try:
        result = await session.execute(text(sql), {"name": common_name})
    except SQLAlchemyError as exc:
        logger.warning("target label join failed, returning unlabelled: %s", exc)
        result = await session.execute(
            text(f"""
                SELECT {_COLUMNS}, '{{}}'::text[] AS target_labels
                FROM mv_chemical_disease_bioactivity m
                WHERE {filter_column} = :name
                {_ORDER}
            """),
            {"name": common_name},
        )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}
