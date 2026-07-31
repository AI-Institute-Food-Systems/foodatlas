"""Disease entity API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import bioactivity_disease, disease, taxonomy

router = APIRouter(
    prefix="/disease",
    dependencies=[Depends(verify_api_key)],
    include_in_schema=False,
)


@router.get("/metadata")
async def disease_metadata(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await disease.get_metadata(db, common_name)


@router.get("/taxonomy")
async def disease_taxonomy(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await taxonomy.get_taxonomy(db, common_name, "disease")


@router.get("/correlation")
async def disease_correlation(
    common_name: str = Query(...),
    page: int = Query(1),
    relation: str = Query("positive"),
    db: AsyncSession = Depends(get_db),
):
    return await disease.get_correlation(db, common_name, page, relation)


@router.get("/chemicals")
async def disease_chemicals(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity_disease.get_disease_chemicals(db, common_name)
