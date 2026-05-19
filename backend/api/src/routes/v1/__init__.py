"""Aggregate router for the public /v1/ API."""

from fastapi import APIRouter, Depends

from src.dependencies import verify_v1_key
from src.rate_limit import enforce_rate_limit

from . import (
    attestations,
    bundles,
    chemicals,
    diseases,
    foods,
    search,
    stats,
    triplets,
)

router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(verify_v1_key), Depends(enforce_rate_limit)],
    tags=["public-v1"],
)
router.include_router(foods.router)
router.include_router(chemicals.router)
router.include_router(diseases.router)
router.include_router(triplets.router)
router.include_router(attestations.router)
router.include_router(search.router)
router.include_router(stats.router)
router.include_router(bundles.router)
