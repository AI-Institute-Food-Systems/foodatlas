"""Download API routes."""

from fastapi import APIRouter, Depends

from src.dependencies import verify_api_key

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.get("/download")
async def get_downloads():
    """Return available download entries.

    For MVP, return a static placeholder.
    Production will read from config or DB.
    """
    return {
        "data": [],
        "metadata": {"row_count": 0},
    }
