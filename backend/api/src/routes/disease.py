"""Disease entity API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import bioactivity_disease, disease, disease_bioactivity, taxonomy

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


@router.get("/chemical-associations")
async def disease_chemical_associations(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity_disease.get_disease_chemical_associations(db, common_name)


@router.get("/bioactivities")
async def disease_bioactivities(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await disease_bioactivity.get_disease_bioactivities(db, common_name)


@router.get("/bioactivity-chemicals")
async def disease_bioactivity_chemicals(
    common_name: str = Query(...),
    bioactivity: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await disease_bioactivity.get_disease_bioactivity_chemicals(
        db, common_name, bioactivity
    )
