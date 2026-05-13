"""Public search endpoint (/v1/search)."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories.v1 import search as search_repo
from src.repositories.v1.pagination import build_page, clamp_page_size
from src.repositories.v1.serializers import ListResponse, SearchHit

router = APIRouter(prefix="/search")

_VALID_TYPES = {"", "food", "chemical", "disease"}


@router.get(
    "",
    response_model=ListResponse[SearchHit],
    summary="Trigram autocomplete across entity names",
)
async def search(
    q: str = Query(..., min_length=1, description="Search term"),
    entity_type: str = Query("", description="Filter to food|chemical|disease"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[SearchHit]:
    if entity_type not in _VALID_TYPES:
        raise HTTPException(
            status_code=422,
            detail="entity_type must be one of food, chemical, disease",
        )
    size = clamp_page_size(page_size)
    rows, total = await search_repo.search(
        db, q=q, entity_type=entity_type, page=page, page_size=size
    )
    return ListResponse[SearchHit](
        data=[SearchHit(**r) for r in rows],
        page=build_page(page, size, total),
    )
