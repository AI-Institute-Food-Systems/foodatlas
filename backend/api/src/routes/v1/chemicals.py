"""Public chemical endpoints (/v1/chemicals)."""

from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories import taxonomy as taxonomy_repo
from src.repositories.v1 import entities, relationships
from src.repositories.v1.pagination import build_page, clamp_page_size
from src.repositories.v1.serializers import (
    BioactivityChemicalRow,
    Chemical,
    ChemicalSummary,
    CompositionRow,
    CorrelationRow,
    ItemResponse,
    ListResponse,
    Taxonomy,
)

router = APIRouter(prefix="/chemicals")


@router.get(
    "",
    response_model=ListResponse[ChemicalSummary],
    summary="List chemicals",
)
async def list_chemicals(
    q: str = Query(""),
    classification: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[ChemicalSummary]:
    size = clamp_page_size(page_size)
    rows, total = await entities.list_entities(
        db, "chemical", q=q, classification=classification, page=page, page_size=size
    )
    return ListResponse[ChemicalSummary](
        data=[ChemicalSummary(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{chemical_id}",
    response_model=ItemResponse[Chemical],
    responses={404: {"description": "Chemical not found"}},
)
async def get_chemical(
    chemical_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Chemical]:
    row = await entities.get_entity(db, "chemical", entity_id=chemical_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Chemical not found")
    return ItemResponse[Chemical](data=Chemical(**row))


@router.get(
    "/{chemical_id}/foods",
    response_model=ListResponse[CompositionRow],
    summary="Foods containing this chemical",
)
async def chemical_foods(
    chemical_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[CompositionRow]:
    size = clamp_page_size(page_size)
    rows, total = await relationships.list_composition(
        db, chemical_id=chemical_id, page=page, page_size=size
    )
    return ListResponse[CompositionRow](
        data=[CompositionRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{chemical_id}/diseases",
    response_model=ListResponse[CorrelationRow],
    summary="Diseases this chemical reduces or worsens",
)
async def chemical_diseases(
    chemical_id: str,
    relation: str = Query(
        "reduces",
        description="'reduces' (r4) or 'worsens' (r3)",
    ),
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
        db,
        chemical_id=chemical_id,
        relation=relation,
        page=page,
        page_size=size,
    )
    return ListResponse[CorrelationRow](
        data=[CorrelationRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{chemical_id}/bioactivities",
    response_model=ListResponse[BioactivityChemicalRow],
    summary="Bioactivities this chemical has been measured for",
)
async def chemical_bioactivities(
    chemical_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[BioactivityChemicalRow]:
    size = clamp_page_size(page_size)
    rows, total = await relationships.list_bioactivity_chemicals(
        db, chemical_id=chemical_id, page=page, page_size=size
    )
    return ListResponse[BioactivityChemicalRow](
        data=[BioactivityChemicalRow(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{chemical_id}/taxonomy",
    response_model=ItemResponse[Taxonomy],
)
async def chemical_taxonomy(
    chemical_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Taxonomy]:
    entity = await entities.get_entity(db, "chemical", entity_id=chemical_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Chemical not found")
    payload = await taxonomy_repo.get_taxonomy(db, entity["common_name"], "chemical")
    return ItemResponse[Taxonomy](data=Taxonomy(**cast("dict", payload["data"])))
