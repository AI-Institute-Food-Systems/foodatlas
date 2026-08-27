"""Chemical entity API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import bioactivity, bioactivity_disease, chemical, taxonomy

router = APIRouter(
    prefix="/chemical",
    dependencies=[Depends(verify_api_key)],
    include_in_schema=False,
)


@router.get("/metadata")
async def chemical_metadata(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await chemical.get_metadata(db, common_name)


@router.get("/taxonomy")
async def chemical_taxonomy(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await taxonomy.get_taxonomy(db, common_name, "chemical")


@router.get("/composition")
async def chemical_composition(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await chemical.get_composition(db, common_name)


@router.get("/composition-evidence")
async def chemical_composition_evidence(
    common_name: str = Query(...),
    food_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Evidence behind one row of /composition, for the row's modal.

    Separate from /composition because the evidence for a whole chemical is
    two orders of magnitude larger than the table that links to it.
    """
    return await chemical.get_composition_evidence(db, common_name, food_name)


@router.get("/correlation")
async def chemical_correlation(
    common_name: str = Query(...),
    page: int = Query(1),
    relation: str = Query("all"),
    search: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    """CTD literature correlations, one page.

    ``relation`` is "all" (both directions, each row carrying its own
    ``relationship_id``), "positive" or "negative".
    """
    return await chemical.get_correlation(db, common_name, page, relation, search)


@router.get("/correlation/direction-counts")
async def chemical_correlation_direction_counts(
    common_name: str = Query(...),
    search: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    """Row counts per direction, for the merged tab's Direction facet."""
    return await chemical.get_correlation_direction_counts(db, common_name, search)


@router.get("/bioactivities")
async def chemical_bioactivities(
    common_name: str = Query(...),
    page: int = Query(1, ge=1),
    search: str = Query(""),
    sort_by: str = Query("measurement_count"),
    sort_dir: str = Query("desc"),
    filter_endpoint: str = Query(""),
    filter_unit: str = Query(""),
    filter_source_kind: str = Query(""),
    filter_evidence_type: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_chemical_bioactivities(
        db,
        common_name,
        page=page,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_source_kind=filter_source_kind,
        filter_evidence_type=filter_evidence_type,
    )


@router.get("/disease-associations")
async def chemical_disease_associations(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity_disease.get_chemical_disease_associations(db, common_name)
