"""Public triplet (graph edge) endpoints (/v1/triplets)."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies import get_db
from src.repositories.v1 import triplets as triplets_repo
from src.repositories.v1.pagination import build_page, clamp_page_size
from src.repositories.v1.serializers import ItemResponse, ListResponse, Triplet

router = APIRouter(prefix="/triplets")


@router.get(
    "",
    response_model=ListResponse[Triplet],
    summary="List knowledge-graph triplets",
)
async def list_triplets(
    head_id: str = Query("", description="Filter by head entity id"),
    tail_id: str = Query("", description="Filter by tail entity id"),
    relationship: str = Query(
        "",
        description=(
            "One of contains|is_a|worsens|reduces, or the raw relationship_id (r1..r5)"
        ),
    ),
    source: str = Query("", description="Filter by source string"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> ListResponse[Triplet]:
    size = clamp_page_size(page_size)
    rows, total = await triplets_repo.list_triplets(
        db,
        head_id=head_id,
        tail_id=tail_id,
        relationship=relationship,
        source=source,
        page=page,
        page_size=size,
    )
    return ListResponse[Triplet](
        data=[Triplet(**r) for r in rows],
        page=build_page(page, size, total),
    )


@router.get(
    "/{triplet_id}",
    response_model=ItemResponse[Triplet],
    responses={404: {"description": "Triplet not found"}},
)
async def get_triplet(
    triplet_id: int, db: AsyncSession = Depends(get_db)
) -> ItemResponse[Triplet]:
    row = await triplets_repo.get_triplet(db, triplet_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Triplet not found")
    return ItemResponse[Triplet](data=Triplet(**row))
