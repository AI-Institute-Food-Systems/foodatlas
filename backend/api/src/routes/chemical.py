"""Chemical entity API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import chemical

router = APIRouter(prefix="/chemical", dependencies=[Depends(verify_api_key)])


@router.get("/metadata")
async def chemical_metadata(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await chemical.get_metadata(db, common_name)


@router.get("/composition")
async def chemical_composition(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await chemical.get_composition(db, common_name)


@router.get("/correlation")
async def chemical_correlation(
    common_name: str = Query(...),
    page: int = Query(1),
    relation: str = Query("positive"),
    db: AsyncSession = Depends(get_db),
):
    return await chemical.get_correlation(db, common_name, page, relation)
