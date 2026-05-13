"""Public bundle-downloads endpoint (/v1/bundles)."""

from fastapi import APIRouter, Depends

from src.config import APISettings
from src.dependencies import get_settings
from src.repositories import downloads
from src.repositories.v1.serializers import Bundle, ListResponse, Page

router = APIRouter(prefix="/bundles")


@router.get(
    "",
    response_model=ListResponse[Bundle],
    summary="List released bulk-download bundles",
)
async def list_bundles(
    settings: APISettings = Depends(get_settings),
) -> ListResponse[Bundle]:
    entries: list[dict] = []
    if settings.downloads_bucket:
        entries = await downloads.fetch_manifest(
            settings.downloads_bucket, settings.downloads_region
        )
    return ListResponse[Bundle](
        data=[Bundle(**e) for e in entries],
        page=Page(
            page=1,
            page_size=max(len(entries), 1),
            total=len(entries),
            has_more=False,
        ),
    )
