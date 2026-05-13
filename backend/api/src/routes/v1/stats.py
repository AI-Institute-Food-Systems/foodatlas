"""Public stats endpoint (/v1/stats)."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories.v1 import search as search_repo
from src.repositories.v1.serializers import ItemResponse, Stats

router = APIRouter(prefix="/stats")


@router.get(
    "",
    response_model=ItemResponse[Stats],
    summary="Aggregate counts (foods, chemicals, diseases, publications, connections)",
)
async def get_stats(db: AsyncSession = Depends(get_db)) -> ItemResponse[Stats]:
    row = await search_repo.get_stats(db)
    return ItemResponse[Stats](data=Stats(**row))
