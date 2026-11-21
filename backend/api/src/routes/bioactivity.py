"""Bioactivity entity API routes."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import bioactivity, bioactivity_facets, disease_bioactivity
from src.repositories.bioactivity_facets import PageFilters

router = APIRouter(
    prefix="/bioactivity",
    dependencies=[Depends(verify_api_key)],
    include_in_schema=False,
)


@router.get("/metadata")
async def bioactivity_metadata(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_metadata(db, common_name)


@router.get("/diseases")
async def bioactivity_diseases(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await disease_bioactivity.get_bioactivity_diseases(db, common_name)


@router.get("/chemicals")
async def bioactivity_chemicals(
    common_name: str = Query(...),
    page: int = Query(1, ge=1),
    search: str = Query(""),
    sort_by: str = Query("measurement_count"),
    sort_dir: str = Query("desc"),
    filter_endpoint: str = Query(""),
    filter_unit: str = Query(""),
    filter_evidence_type: str = Query(""),
    filter_source_kind: str = Query(""),
    filter_category: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_chemicals(
        db,
        common_name,
        page=page,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
        filter_category=filter_category,
    )


@router.get("/foods")
async def bioactivity_foods(
    common_name: str = Query(...),
    page: int = Query(1, ge=1),
    search: str = Query(""),
    sort_by: str = Query("measurement_count"),
    sort_dir: str = Query("desc"),
    filter_endpoint: str = Query(""),
    filter_unit: str = Query(""),
    filter_evidence_type: str = Query(""),
    filter_source_kind: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    return await bioactivity.get_foods(
        db,
        common_name,
        page=page,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filter_endpoint=filter_endpoint,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
        filter_source_kind=filter_source_kind,
    )


_DIRECTION_DOC = (
    "Pivot+relationship combo. One of: bioactivity-chemicals, bioactivity-foods, "
    "chemical-bioactivities, food-bioactivities, food-inferred-bioactivities."
)


def _page_filters(
    search: str = Query(""),
    filter_evidence_type: str = Query(""),
    filter_unit: str = Query(""),
    filter_source_kind: str = Query(""),
    filter_category: str = Query(""),
) -> PageFilters:
    """The page's active filters. Each facet skips its own dimension."""
    return PageFilters(
        search=search,
        evidence_type=filter_evidence_type,
        unit=filter_unit,
        source_kind=filter_source_kind,
        category=filter_category,
    )


@router.get("/endpoints")
async def bioactivity_endpoint_options(
    common_name: str = Query(...),
    direction: str = Query(..., description=_DIRECTION_DOC),
    filters: PageFilters = Depends(_page_filters),
    db: AsyncSession = Depends(get_db),
):
    """Per-unit row counts for the sidebar Unit filter, other filters applied."""
    return await bioactivity_facets.get_unit_counts(db, common_name, direction, filters)


@router.get("/categories")
async def bioactivity_category_options(
    common_name: str = Query(..., description="Bioactivity common_name"),
    filters: PageFilters = Depends(_page_filters),
    db: AsyncSession = Depends(get_db),
):
    """Per-category row counts on the Chemicals tab, other filters applied."""
    return await bioactivity_facets.get_category_counts(db, common_name, filters)


@router.get("/source_kinds")
async def bioactivity_source_kind_counts(
    common_name: str = Query(...),
    direction: str = Query(..., description=_DIRECTION_DOC),
    filters: PageFilters = Depends(_page_filters),
    db: AsyncSession = Depends(get_db),
):
    """Per-source-kind row counts for the sidebar Assay Source filter."""
    return await bioactivity_facets.get_source_kind_counts(
        db, common_name, direction, filters
    )


@router.get("/evidence_types")
async def bioactivity_evidence_type_counts(
    common_name: str = Query(...),
    direction: str = Query(..., description=_DIRECTION_DOC),
    filters: PageFilters = Depends(_page_filters),
    db: AsyncSession = Depends(get_db),
):
    """Per-evidence-type row counts for the sidebar Evidence filter."""
    return await bioactivity_facets.get_evidence_type_counts(
        db, common_name, direction, filters
    )


@router.get("/measurements")
async def bioactivity_measurements(
    head_id: str = Query(..., description="Chemical or food foodatlas_id"),
    tail_id: str = Query(..., description="Bioactivity foodatlas_id"),
    relationship: str = Query(
        ...,
        description=(
            "'r6' for (chemical, bioactivity) pairs, 'r5' for "
            "(food, bioactivity) pairs."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    """Full unbounded measurement list for a single (head, bioactivity) pair."""
    return await bioactivity.get_measurements(db, head_id, tail_id, relationship)
