"""Bioactivity entity repository (internal SSR consumer).

Returns rich, UI-shaped payloads — full ``measurements``/``evidences`` JSONB,
ambiguity_siblings, etc. The flat /v1/ surface lives in
``repositories.v1.bioactivity``.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .formatting import format_external_ids

ROWS_PER_PAGE = 10


async def get_metadata(session: AsyncSession, common_name: str) -> dict[str, object]:
    """Bioactivity metadata for the detail page."""
    result = await session.execute(
        text("""
            SELECT common_name, foodatlas_id AS id, entity_type,
                   scientific_name, synonyms, external_ids, description,
                   ambiguity_siblings
            FROM mv_bioactivity_entities WHERE common_name = :name
        """),
        {"name": common_name},
    )
    data = [dict(row._mapping) for row in result]
    for row in data:
        row["external_ids"] = format_external_ids(row.get("external_ids"))
    return {"data": data, "metadata": {"row_count": len(data)}}


async def get_chemicals(
    session: AsyncSession, common_name: str, page: int = 1
) -> dict[str, object]:
    """Chemicals measured against this bioactivity, with raw assay JSONB."""
    offset = ROWS_PER_PAGE * (page - 1)
    result = await session.execute(
        text("""
            SELECT chemical_foodatlas_id AS id, chemical_name AS name,
                   measurement_count, measurements
            FROM mv_chemical_bioactivity_measurement
            WHERE bioactivity_name = :name
            ORDER BY measurement_count DESC
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {"name": common_name, "offset": offset, "limit": ROWS_PER_PAGE},
    )
    data = [dict(r._mapping) for r in result]

    count_result = await session.execute(
        text(
            "SELECT COUNT(*) FROM mv_chemical_bioactivity_measurement"
            " WHERE bioactivity_name = :name"
        ),
        {"name": common_name},
    )
    total_rows = count_result.scalar() or 0
    return _page_payload(data, page, total_rows)


async def get_foods(
    session: AsyncSession,
    common_name: str,
    page: int = 1,
    exhibit_type: str = "all",
) -> dict[str, object]:
    """Foods exhibiting this bioactivity, with direct/inherited filter."""
    offset = ROWS_PER_PAGE * (page - 1)
    where = "bioactivity_name = :name"
    params: dict[str, object] = {
        "name": common_name,
        "offset": offset,
        "limit": ROWS_PER_PAGE,
    }
    if exhibit_type in ("direct", "inherited"):
        where += " AND exhibit_type = :et"
        params["et"] = exhibit_type

    result = await session.execute(
        text(f"""
            SELECT food_foodatlas_id AS id, food_name AS name,
                   exhibit_type, via_chemical_id, via_chemical_name,
                   efficacy_pred, evidence_count, evidences
            FROM mv_food_bioactivity_exhibits
            WHERE {where}
            ORDER BY evidence_count DESC
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        params,
    )
    data = [dict(r._mapping) for r in result]

    count_params = {k: v for k, v in params.items() if k in ("name", "et")}
    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM mv_food_bioactivity_exhibits WHERE {where}"),
        count_params,
    )
    total_rows = count_result.scalar() or 0
    return _page_payload(data, page, total_rows)


async def get_diseases(
    session: AsyncSession, common_name: str, page: int = 1
) -> dict[str, object]:
    """Diseases associated with this bioactivity, with target ids + evidences."""
    offset = ROWS_PER_PAGE * (page - 1)
    result = await session.execute(
        text("""
            SELECT disease_foodatlas_id AS id, disease_name AS name,
                   polarity, target_ids, evidence_count, evidences
            FROM mv_bioactivity_disease_association
            WHERE bioactivity_name = :name
            ORDER BY evidence_count DESC
            OFFSET :offset ROWS FETCH FIRST :limit ROWS ONLY
        """),
        {"name": common_name, "offset": offset, "limit": ROWS_PER_PAGE},
    )
    data = [dict(r._mapping) for r in result]

    count_result = await session.execute(
        text(
            "SELECT COUNT(*) FROM mv_bioactivity_disease_association"
            " WHERE bioactivity_name = :name"
        ),
        {"name": common_name},
    )
    total_rows = count_result.scalar() or 0
    return _page_payload(data, page, total_rows)


def _page_payload(data: list, page: int, total_rows: int) -> dict[str, object]:
    """Wrap rows in the SSR payload shape (mirrors other internal repos)."""
    total_pages = (total_rows + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE if total_rows else 0
    offset = ROWS_PER_PAGE * (page - 1)
    return {
        "data": data,
        "metadata": {
            "row_count": len(data),
            "rows_per_page": ROWS_PER_PAGE,
            "current_row": offset + 1,
            "current_page": page,
            "total_rows": total_rows,
            "total_pages": total_pages,
        },
    }
