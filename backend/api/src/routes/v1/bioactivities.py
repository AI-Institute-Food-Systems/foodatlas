"""Public bioactivity endpoints (/v1/bioactivities)."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories.v1 import bioactivity, entities
from src.repositories.v1.pagination import build_page, clamp_page_size
from src.repositories.v1.serializers import (
    Bioactivity,
    BioactivityDiseaseRow,
    BioactivityExhibitRow,
    BioactivityMeasurementRow,
    BioactivitySummary,
    ItemResponse,
    ListResponse,
)

router = APIRouter(prefix="/bioactivities")

_VALID_EXHIBIT_TYPES = {"all", "direct", "inherited"}


@router.get(
    "",
    response_model=ListResponse[BioactivitySummary],
    summary="List bioactivities",
)
async def list_bioactivities(
    q: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivitySummary]:
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
    response_model=ListResponse[BioactivityMeasurementRow],
    summary="Chemicals measured against this bioactivity",
)
async def bioactivity_chemicals(
    bioactivity_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivityMeasurementRow]:
    size = clamp_page_size(page_size)
    rows, total = await bioactivity.list_measurements(
        db, bioactivity_id=bioactivity_id, page=page, page_size=size
    )
    return ListResponse[BioactivityMeasurementRow](
        data=[BioactivityMeasurementRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{bioactivity_id}/foods",
    response_model=ListResponse[BioactivityExhibitRow],
    summary="Foods exhibiting this bioactivity",
)
async def bioactivity_foods(
    bioactivity_id: str,
    exhibit_type: str = Query(
        "all", description="'direct', 'inherited', or 'all' (default)"
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivityExhibitRow]:
    if exhibit_type not in _VALID_EXHIBIT_TYPES:
        raise HTTPException(
            status_code=422,
            detail="exhibit_type must be one of direct, inherited, all",
        )
    size = clamp_page_size(page_size)
    rows, total = await bioactivity.list_exhibits(
        db,
        bioactivity_id=bioactivity_id,
        exhibit_type=exhibit_type,
        page=page,
        page_size=size,
    )
    return ListResponse[BioactivityExhibitRow](
        data=[BioactivityExhibitRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{bioactivity_id}/diseases",
    response_model=ListResponse[BioactivityDiseaseRow],
    summary="Diseases associated with this bioactivity",
)
async def bioactivity_diseases(
    bioactivity_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivityDiseaseRow]:
    size = clamp_page_size(page_size)
    rows, total = await bioactivity.list_associations(
        db, bioactivity_id=bioactivity_id, page=page, page_size=size
    )
    return ListResponse[BioactivityDiseaseRow](
        data=[BioactivityDiseaseRow(**r) for r in rows],
        page=build_page(page, size, total),
    )
