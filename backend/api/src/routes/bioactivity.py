"""Bioactivity entity API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import bioactivity

router = APIRouter(
    prefix="/bioactivity",
    dependencies=[Depends(verify_api_key)],
    include_in_schema=False,
)


@router.get("/metadata")
async def bioactivity_metadata(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_metadata(db, common_name)


@router.get("/chemicals")
async def bioactivity_chemicals(
    common_name: str = Query(...),
    page: int = Query(1, ge=1),
    search: str = Query(""),
    sort_by: str = Query("measurement_count"),
    sort_dir: str = Query("desc"),
    filter_endpoint: str = Query(""),
    filter_unit: str = Query(""),
    filter_evidence_type: str = Query(""),
    filter_source_kind: str = Query(""),
    filter_category: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_chemicals(
        db,
        common_name,
        page=page,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
        filter_category=filter_category,
    )


@router.get("/foods")
async def bioactivity_foods(
    common_name: str = Query(...),
    page: int = Query(1, ge=1),
    search: str = Query(""),
    sort_by: str = Query("measurement_count"),
    sort_dir: str = Query("desc"),
    filter_endpoint: str = Query(""),
    filter_unit: str = Query(""),
    filter_evidence_type: str = Query(""),
    filter_source_kind: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_foods(
        db,
        common_name,
        page=page,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
    )


@router.get("/endpoints")
async def bioactivity_endpoint_options(
    common_name: str = Query(...),
    direction: str = Query(
        ...,
        description=(
            "Pivot+relationship combo. One of: bioactivity-chemicals, "
            "bioactivity-foods, chemical-bioactivities, food-bioactivities."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    """Distinct (endpoint, unit, count) tuples for the table's filter UI."""
    return await bioactivity.get_endpoint_options(db, common_name, direction)


@router.get("/measurements")
async def bioactivity_measurements(
    head_id: str = Query(..., description="Chemical or food foodatlas_id"),
    tail_id: str = Query(..., description="Bioactivity foodatlas_id"),
    relationship: str = Query(
        ...,
        description=(
            "'r6' for (chemical, bioactivity) pairs, 'r5' for "
            "(food, bioactivity) pairs."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    """Full unbounded measurement list for a single (head, bioactivity) pair."""
    return await bioactivity.get_measurements(db, head_id, tail_id, relationship)
