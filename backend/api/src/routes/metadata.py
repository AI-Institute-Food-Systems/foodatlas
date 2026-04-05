"""Search and statistics API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import search as search_repo

router = APIRouter(prefix="/metadata", dependencies=[Depends(verify_api_key)])


@router.get("/search")
async def search(
    term: str = Query(""),
    page: int = Query(1),
    db: AsyncSession = Depends(get_db),
):
    return await search_repo.search(db, term, page)


@router.get("/statistics")
async def statistics(
    db: AsyncSession = Depends(get_db),
):
    return await search_repo.get_statistics(db)
