"""Search and statistics API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import entity_index
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


@router.get("/entities")
async def entities(
    entity_type: entity_index.IndexEntityType | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> dict[str, list[dict[str, str]]]:
    """Every entity with a page, for the frontend sitemap (unpaginated).

    ``entity_type`` returns one type only; the chemical list alone is tens of
    thousands of rows, so each per-type sitemap asks for its own.
    """
    return {"data": await entity_index.list_entities(db, entity_type)}


@router.get("/statistics")
async def statistics(
    db: AsyncSession = Depends(get_db),
):
    return await search_repo.get_statistics(db)
