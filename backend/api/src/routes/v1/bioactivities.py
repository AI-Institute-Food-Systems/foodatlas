"""Public bioactivity endpoints (/v1/bioactivities).

Bioactivities are ontology concepts (e.g. "antioxidant") that link back to
chemicals via r6 (MEASURED) and to foods via r5 (EXHIBITS). The MV-backed
list endpoints return one row per (bioactivity, chemical|food) pair with a
sample of measurements and a computed ``top_measurement``.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories.v1 import entities, relationships
from src.repositories.v1.pagination import build_page, clamp_page_size
from src.repositories.v1.serializers import (
    Bioactivity,
    BioactivityChemicalRow,
    BioactivityFoodRow,
    BioactivitySummary,
    ItemResponse,
    ListResponse,
)

router = APIRouter(prefix="/bioactivities")


@router.get(
    "",
    response_model=ListResponse[BioactivitySummary],
    summary="List bioactivities",
)
async def list_bioactivities(
    q: str = Query("", description="Case-insensitive substring filter on common_name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivitySummary]:
    """Paginated list of bioactivity concept entities."""
    size = clamp_page_size(page_size)
    rows, total = await entities.list_entities(
        db, "bioactivity", q=q, page=page, page_size=size
    )
    return ListResponse[BioactivitySummary](
        data=[BioactivitySummary(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{bioactivity_id}",
    response_model=ItemResponse[Bioactivity],
    summary="Get one bioactivity by FoodAtlas id",
    responses={404: {"description": "Bioactivity not found"}},
)
async def get_bioactivity(
    bioactivity_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Bioactivity]:
    row = await entities.get_entity(db, "bioactivity", entity_id=bioactivity_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Bioactivity not found")
    return ItemResponse[Bioactivity](data=Bioactivity(**row))


@router.get(
    "/{bioactivity_id}/chemicals",
    response_model=ListResponse[BioactivityChemicalRow],
    summary="Chemicals measured for this bioactivity",
)
async def bioactivity_chemicals(
    bioactivity_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivityChemicalRow]:
    size = clamp_page_size(page_size)
    rows, total = await relationships.list_bioactivity_chemicals(
        db, bioactivity_id=bioactivity_id, page=page, page_size=size
    )
    return ListResponse[BioactivityChemicalRow](
        data=[BioactivityChemicalRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{bioactivity_id}/foods",
    response_model=ListResponse[BioactivityFoodRow],
    summary="Foods that exhibit this bioactivity",
)
async def bioactivity_foods(
    bioactivity_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivityFoodRow]:
    size = clamp_page_size(page_size)
    rows, total = await relationships.list_bioactivity_foods(
        db, bioactivity_id=bioactivity_id, page=page, page_size=size
    )
    return ListResponse[BioactivityFoodRow](
        data=[BioactivityFoodRow(**r) for r in rows],
        page=build_page(page, size, total),
    )
