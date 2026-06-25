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
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_chemicals(
        db, common_name, page=page, search=search,
        sort_by=sort_by, sort_dir=sort_dir,
    )


@router.get("/foods")
async def bioactivity_foods(
    common_name: str = Query(...),
    page: int = Query(1, ge=1),
    search: str = Query(""),
    sort_by: str = Query("measurement_count"),
    sort_dir: str = Query("desc"),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_foods(
        db, common_name, page=page, search=search,
        sort_by=sort_by, sort_dir=sort_dir,
    )


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
