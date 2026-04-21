"""Download API routes."""

from fastapi import APIRouter, Depends

from src.config import APISettings
from src.dependencies import get_settings, verify_api_key
from src.repositories import downloads

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.get("/download")
async def get_downloads(settings: APISettings = Depends(get_settings)) -> dict:
    """Return released bundles from the public downloads bucket manifest.

    Returns an empty list when no bucket is configured or when the
    manifest is missing/unreadable — the downloads page renders an
    empty table in that case rather than failing.
    """
    entries: list[dict] = []
    if settings.downloads_bucket:
        entries = await downloads.fetch_manifest(
            settings.downloads_bucket,
            settings.downloads_region,
        )
    return {"data": entries, "metadata": {"row_count": len(entries)}}
