"""Search and statistics API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import search as search_repo

router = APIRouter(
    prefix="/metadata",
    dependencies=[Depends(verify_api_key)],
    include_in_schema=False,
)


@router.get("/search")
async def search(
    term: str = Query(""),
    page: int = Query(1),
    # Caller-controlled page size. Capped at 100 so a bad client can't
    # drag a MV-scan-per-request into DoS territory. Default keeps
    # legacy behavior for anyone still on the old client.
    rows_per_page: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    return await search_repo.search(db, term, page, rows_per_page)


@router.get("/statistics")
async def statistics(
    db: AsyncSession = Depends(get_db),
):
    return await search_repo.get_statistics(db)
