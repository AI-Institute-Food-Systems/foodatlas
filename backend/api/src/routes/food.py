"""Food entity API routes."""

from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key
from src.repositories import food, taxonomy

if TYPE_CHECKING:
    from src.repositories.trust_filter import TrustMode

router = APIRouter(prefix="/food", dependencies=[Depends(verify_api_key)])

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
    db: AsyncSession = Depends(get_db),
):
    return await food.get_composition_counts(db, common_name)


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
    )
