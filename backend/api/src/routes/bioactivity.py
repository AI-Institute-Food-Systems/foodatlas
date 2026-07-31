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
    page: int = Query(1),
    limit: int = Query(50),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_chemicals(db, common_name, page, limit)


@router.get("/foods")
async def bioactivity_foods(
    common_name: str = Query(...),
    page: int = Query(1),
    limit: int = Query(50),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_foods(db, common_name, page, limit)
