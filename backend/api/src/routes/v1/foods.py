"""Public food endpoints (/v1/foods)."""

from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories import taxonomy as taxonomy_repo
from src.repositories.v1 import entities, relationships
from src.repositories.v1.pagination import build_page, clamp_page_size
from src.repositories.v1.serializers import (
    CompositionRow,
    Food,
    FoodSummary,
    ItemResponse,
    ListResponse,
    Taxonomy,
)

router = APIRouter(prefix="/foods")


@router.get("", response_model=ListResponse[FoodSummary], summary="List foods")
async def list_foods(
    q: str = Query("", description="Case-insensitive substring filter on common_name"),
    classification: str = Query(
        "", description="Exact match against food_classification (e.g. 'fruit')"
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[FoodSummary]:
    """Paginated list of food entities."""
    size = clamp_page_size(page_size)
    rows, total = await entities.list_entities(
        db, "food", q=q, classification=classification, page=page, page_size=size
    )
    return ListResponse[FoodSummary](
        data=[FoodSummary(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{food_id}",
    response_model=ItemResponse[Food],
    summary="Get one food by FoodAtlas id",
    responses={404: {"description": "Food not found"}},
)
async def get_food(
    food_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Food]:
    row = await entities.get_entity(db, "food", entity_id=food_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Food not found")
    return ItemResponse[Food](data=Food(**row))


@router.get(
    "/{food_id}/chemicals",
    response_model=ListResponse[CompositionRow],
    summary="Chemicals contained in a food",
)
async def food_chemicals(
    food_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[CompositionRow]:
    """Flat composition rows: one chemical per row, evidence aggregated."""
    size = clamp_page_size(page_size)
    rows, total = await relationships.list_composition(
        db, food_id=food_id, page=page, page_size=size
    )
    return ListResponse[CompositionRow](
        data=[CompositionRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{food_id}/taxonomy",
    response_model=ItemResponse[Taxonomy],
    summary="IS_A taxonomy ancestry for a food",
)
async def food_taxonomy(
    food_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Taxonomy]:
    entity = await entities.get_entity(db, "food", entity_id=food_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Food not found")
    payload = await taxonomy_repo.get_taxonomy(db, entity["common_name"], "food")
    return ItemResponse[Taxonomy](data=Taxonomy(**cast("dict", payload["data"])))
