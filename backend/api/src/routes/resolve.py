"""Resolve entity ID to common_name for URL redirects."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db, verify_api_key

router = APIRouter(prefix="/resolve", dependencies=[Depends(verify_api_key)])


@router.get("")
async def resolve_entity(
    entity_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return entity_type and common_name for a given foodatlas_id."""
    result = await db.execute(
        text(
            "SELECT entity_type, common_name FROM base_entities"
            " WHERE foodatlas_id = :id"
        ),
        {"id": entity_id},
    )
    row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Entity not found.")
    return {"entity_type": row[0], "common_name": row[1]}
