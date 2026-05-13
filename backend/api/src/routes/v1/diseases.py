"""Public disease endpoints (/v1/diseases)."""

from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories import taxonomy as taxonomy_repo
from src.repositories.v1 import entities, relationships
from src.repositories.v1.pagination import build_page, clamp_page_size
from src.repositories.v1.serializers import (
    CorrelationRow,
    Disease,
    DiseaseSummary,
    ItemResponse,
    ListResponse,
    Taxonomy,
)

router = APIRouter(prefix="/diseases")


@router.get("", response_model=ListResponse[DiseaseSummary])
async def list_diseases(
    q: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[DiseaseSummary]:
    size = clamp_page_size(page_size)
    rows, total = await entities.list_entities(
        db, "disease", q=q, page=page, page_size=size
    )
    return ListResponse[DiseaseSummary](
        data=[DiseaseSummary(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{disease_id}",
    response_model=ItemResponse[Disease],
    responses={404: {"description": "Disease not found"}},
)
async def get_disease(
    disease_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Disease]:
    row = await entities.get_entity(db, "disease", entity_id=disease_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Disease not found")
    return ItemResponse[Disease](data=Disease(**row))


@router.get(
    "/{disease_id}/chemicals",
    response_model=ListResponse[CorrelationRow],
    summary="Chemicals that reduce or worsen this disease",
)
async def disease_chemicals(
    disease_id: str,
    relation: str = Query("reduces"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[CorrelationRow]:
    if relation not in {"reduces", "worsens"}:
        raise HTTPException(
            status_code=422, detail="relation must be reduces or worsens"
        )
    size = clamp_page_size(page_size)
    rows, total = await relationships.list_correlation(
        db, disease_id=disease_id, relation=relation, page=page, page_size=size
    )
    return ListResponse[CorrelationRow](
        data=[CorrelationRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{disease_id}/taxonomy",
    response_model=ItemResponse[Taxonomy],
)
async def disease_taxonomy(
    disease_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Taxonomy]:
    entity = await entities.get_entity(db, "disease", entity_id=disease_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Disease not found")
    payload = await taxonomy_repo.get_taxonomy(db, entity["common_name"], "disease")
    return ItemResponse[Taxonomy](data=Taxonomy(**cast("dict", payload["data"])))
