"""Public attestation endpoints (/v1/attestations)."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories.v1 import triplets as triplets_repo
from src.repositories.v1.serializers import Attestation, ItemResponse

router = APIRouter(prefix="/attestations")


@router.get(
    "/{attestation_id}",
    response_model=ItemResponse[Attestation],
    summary="Get one attestation (raw evidence row backing a triplet)",
    responses={404: {"description": "Attestation not found"}},
)
async def get_attestation(
    attestation_id: str, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Attestation]:
    row = await triplets_repo.get_attestation(db, attestation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Attestation not found")
    return ItemResponse[Attestation](data=Attestation(**row))
