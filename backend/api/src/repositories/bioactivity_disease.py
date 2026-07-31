"""Chemical↔disease (bioactivity-inferred) repository.

Reads only ``mv_chemical_disease_bioactivity`` — associations inferred from a
chemical being *active* in assays the bioactivity-disease bridge ties to a
disease. Distinct from ``/chemical/correlation`` (CTD literature correlations).
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_COLUMNS = """
    chemical_name, chemical_foodatlas_id, disease_name, disease_foodatlas_id,
    n_assays, n_active_measurements, relationships, target_genes, assays
"""


async def get_chemical_disease_associations(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Diseases associated with a chemical (most shared assays first)."""
    result = await session.execute(
        text(f"""
            SELECT {_COLUMNS}
            FROM mv_chemical_disease_bioactivity WHERE chemical_name = :name
            ORDER BY n_assays DESC, n_active_measurements DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_disease_chemical_associations(
    session: AsyncSession, common_name: str
) -> dict[str, object]:
    """Chemicals associated with a disease (most shared assays first)."""
    result = await session.execute(
        text(f"""
            SELECT {_COLUMNS}
            FROM mv_chemical_disease_bioactivity WHERE disease_name = :name
            ORDER BY n_assays DESC, n_active_measurements DESC
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    return {"data": data, "metadata": {"row_count": len(data)}}
