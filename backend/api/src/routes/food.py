"""Food entity API routes."""

from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import bioactivity, efficacy, food, taxonomy

if TYPE_CHECKING:
    from src.repositories.trust_filter import TrustMode

router = APIRouter(
    prefix="/food",
    dependencies=[Depends(verify_api_key)],
    include_in_schema=False,
)

_VALID_TRUST_MODES = ("default", "show_all", "low_only")


@router.get("/metadata")
async def food_metadata(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await food.get_metadata(db, common_name)


@router.get("/taxonomy")
async def food_taxonomy(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await taxonomy.get_taxonomy(db, common_name, "food")


@router.get("/profile")
async def food_profile(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await food.get_profile(db, common_name)


@router.get("/composition/counts")
async def food_composition_counts(
    common_name: str = Query(...),
    filter_source: str = Query(""),
    filter_classification: str = Query(""),
    show_all_rows: str = Query("true"),
    trust: str = Query("default"),
    search: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    """Faceted composition counts.

    Every filter parameter matches the shape /food/composition takes so
    the counts endpoint can mirror the same view — each per-dimension
    count applies the *other* filters and reports how many rows the
    dimension currently governs.
    """
    trust_mode = cast("TrustMode", trust if trust in _VALID_TRUST_MODES else "default")
    return await food.get_composition_counts(
        db,
        common_name,
        filter_source=filter_source,
        filter_classification=filter_classification,
        show_all_rows=show_all_rows.lower() != "false",
        trust=trust_mode,
        search_term=search,
    )


@router.get("/composition")
async def food_composition(
    common_name: str = Query(...),
    page: int = Query(1),
    filter_source: str = Query(""),
    search: str = Query(""),
    sort_by: str = Query("common_name"),
    sort_dir: str = Query("desc"),
    show_all_rows: str = Query("true"),
    filter_classification: str = Query(""),
    trust: str = Query(
        "default",
        description=(
            "Per-attestation trust filter. 'default' hides low-trust "
            "extractions; 'show_all' returns everything; 'low_only' returns "
            "only low-trust extractions."
        ),
    ),
    find_chemical: str = Query(
        "",
        description=(
            "Locate a chemical by common name (case-insensitive) in the "
            "unfiltered sorted list and serve the page containing it. The "
            "served page overrides the `page` param when a match is found; "
            "metadata.highlight_page reports the resolved page number."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    show_all = show_all_rows.lower() != "false"
    trust_mode = cast("TrustMode", trust if trust in _VALID_TRUST_MODES else "default")
    return await food.get_composition(
        db,
        common_name,
        page,
        filter_source,
        search,
        sort_by,
        sort_dir,
        show_all,
        filter_classification,
        trust=trust_mode,
        find_chemical=find_chemical,
    )


@router.get("/bioactivities")
async def food_bioactivities(
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
    return await bioactivity.get_food_bioactivities(
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


@router.get("/inferred-bioactivities")
async def food_inferred_bioactivities(
    common_name: str = Query(...),
    page: int = Query(1, ge=1),
    search: str = Query(""),
    sort_by: str = Query("concentration"),
    sort_dir: str = Query("desc"),
    filter_source_kind: str = Query(""),
    filter_unit: str = Query(""),
    filter_evidence_type: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    """Bioactivities inferred via the food's chemical composition.

    Joins mv_food_chemical_composition x mv_chemical_bioactivity on
    chemical_foodatlas_id. One row per (chemical, bioactivity) pair —
    the chemical's food-level concentration is included so the UI can
    surface "how much is in it" alongside the inferred bioactivity.
    """
    return await bioactivity.get_food_inferred_bioactivities(
        db,
        common_name,
        page=page,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filter_source_kind=filter_source_kind,
        filter_unit=filter_unit,
        filter_evidence_type=filter_evidence_type,
    )


@router.get("/efficacy")
async def food_efficacy(
    common_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    return await efficacy.get_food_efficacy(db, common_name)
