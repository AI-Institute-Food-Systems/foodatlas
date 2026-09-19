"""Public bundle-downloads endpoints (/v1/bundles).

Listing is metadata only. The zip behind each entry is private; a key
holder fetches it through ``/v1/bundles/{version}/download``, which
answers with a 302 to a pre-signed S3 URL good for a few minutes. Putting
the hop behind the same key, rate limit and access log as the rest of
``/v1`` is what makes "downloads are gated like the API" literally true.
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse

from src.config import APISettings
from src.dependencies import get_settings
from src.repositories import downloads
from src.repositories.v1.serializers import Bundle, ListResponse, Page

router = APIRouter(prefix="/bundles")


def download_url(settings: APISettings, version: str) -> str:
    return f"{settings.public_url.rstrip('/')}/v1/bundles/{version}/download"


async def _entries(settings: APISettings) -> list[dict]:
    if not settings.downloads_bucket:
        return []
    return await downloads.fetch_manifest(
        settings.downloads_bucket, settings.downloads_region
    )


@router.get(
    "",
    response_model=ListResponse[Bundle],
    summary="List released bulk-download bundles",
)
async def list_bundles(
    settings: APISettings = Depends(get_settings),
) -> ListResponse[Bundle]:
    entries = await _entries(settings)
    return ListResponse[Bundle](
        data=[
            Bundle(**{**e, "download_link": download_url(settings, str(e["version"]))})
            for e in entries
        ],
        page=Page(
            page=1,
            page_size=max(len(entries), 1),
            total=len(entries),
            has_more=False,
        ),
    )


@router.get(
    "/{version}/download",
    summary="Download one bundle (302 to a short-lived pre-signed URL)",
    status_code=302,
    response_class=RedirectResponse,
    responses={404: {"description": "Unknown bundle version"}},
)
async def download_bundle(
    version: str,
    settings: APISettings = Depends(get_settings),
) -> RedirectResponse:
    entry = downloads.find_bundle(await _entries(settings), version)
    if entry is None:
        raise HTTPException(status_code=404, detail="Unknown bundle version")
    url = downloads.presign_bundle(
        entry,
        bucket=settings.downloads_bucket,
        region=settings.downloads_region,
        expires_seconds=settings.downloads_presign_seconds,
    )
    return RedirectResponse(url, status_code=302)
