"""Chemical↔disease (bioactivity-inferred) repository.

Reads only ``mv_chemical_disease_bioactivity`` — associations inferred from a
chemical being *active* in assays the bioactivity-disease bridge ties to a
disease. Distinct from ``/chemical/correlation`` (CTD literature correlations).

Beyond the raw counts, each row carries three things worth rendering:

* ``relationships`` — the CTD direct-evidence class(es) the bridge assigned:
  ``therapeutic`` (the chemical mitigates the disease) or ``marker/mechanism``
  (it marks or drives it). Those point in opposite directions, so a UI that
  flattens them into one "associated with" throws away the useful bit.
* ``targets`` — the assay's protein target, i.e. what the link runs *through*,
  as ``{id, label}`` pairs.
* ``literature_directions`` — the same two-value vocabulary sourced from CTD
  literature instead of the assay bridge, so the two can be compared. Empty
  for most rows; that is expected, and is why a match is worth flagging.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .target_labels import attach_targets

_COLUMNS = """
    chemical_name, chemical_foodatlas_id, disease_name, disease_foodatlas_id,
    n_assays, n_active_measurements, relationships, target_genes, assays,
    literature_directions
"""


async def get_chemical_disease_associations(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Diseases associated with a chemical (most shared assays first)."""
    return await _associations(session, "chemical_name", common_name)


async def get_disease_chemical_associations(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Chemicals associated with a disease (most shared assays first)."""
    return await _associations(session, "disease_name", common_name)


async def _associations(
    session: AsyncSession, filter_column: str, common_name: str
) -> dict[str, object]:
    """One direction of the association query, with its targets labelled.

    ``filter_column`` is chosen by the caller from the two literals above, never
    taken from request input.
    """
    result = await session.execute(
        text(f"""
            SELECT {_COLUMNS}
            FROM mv_chemical_disease_bioactivity WHERE {filter_column} = :name
            ORDER BY n_assays DESC, n_active_measurements DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    await attach_targets(session, data)
    return {"data": data, "metadata": {"row_count": len(data)}}
