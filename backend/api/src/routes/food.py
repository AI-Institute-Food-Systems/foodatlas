"""Food entity API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import food

router = APIRouter(prefix="/food", dependencies=[Depends(verify_api_key)])


@router.get("/metadata")
async def food_metadata(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await food.get_metadata(db, common_name)


@router.get("/profile")
async def food_profile(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await food.get_profile(db, common_name)


@router.get("/composition/counts")
async def food_composition_counts(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await food.get_composition_counts(db, common_name)


@router.get("/composition")
async def food_composition(
    common_name: str = Query(...),
    page: int = Query(1),
    filter_source: str = Query(""),
    search: str = Query(""),
    sort_by: str = Query("common_name"),
    sort_dir: str = Query("desc"),
    show_all_rows: str = Query("true"),
    filter_classification: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    show_all = show_all_rows.lower() != "false"
    return await food.get_composition(
        db,
        common_name,
        page,
        filter_source,
        search,
        sort_by,
        sort_dir,
        show_all,
        filter_classification,
    )
