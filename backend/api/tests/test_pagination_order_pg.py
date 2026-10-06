"""Paging through a list serves every row exactly once, against real Postgres.

Every fixture row ties on the sort value and names repeat across ids.
Without a unique key at the end of the ORDER BY, Postgres may order tied
rows differently for each OFFSET, so one row shows on two pages and
another never shows (8,217 of 23,032 antiviral chemicals on 2026-10-05).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import text
from src.repositories import bioactivity, chemical, disease

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.asyncio

_N = 90
_PER_PAGE = 7

_CHEM_BIOACT_INSERT = text(
    "INSERT INTO mv_chemical_bioactivity (bioactivity_name,"
    " bioactivity_foodatlas_id, chemical_name, chemical_foodatlas_id,"
    " measurement_count, measurements)"
    " VALUES (:bn, :bid, :cn, :cid, 2, '[]'::jsonb)"
)

_CORRELATION_INSERT = text(
    "INSERT INTO mv_chemical_disease_correlation (id, chemical_name,"
    " chemical_foodatlas_id, relationship_id, disease_name,"
    " disease_foodatlas_id, evidence_count)"
    " VALUES (:i, :cn, :cid, 'r4', :dn, :did, 1)"
)


def _ids(payload: dict[str, Any]) -> list[str]:
    data = payload["data"]
    rows = data["associations"] if isinstance(data, dict) else data
    return [r["id"] for r in rows]


async def _walk(fetch: Callable[[int], Awaitable[dict[str, Any]]]) -> list[str]:
    first = await fetch(1)
    seen = _ids(first)
    for page in range(2, first["metadata"]["total_pages"] + 1):
        seen += _ids(await fetch(page))
    return seen


def _assert_each_once(seen: list[str], expected: set[str]) -> None:
    dupes = len(seen) - len(set(seen))
    assert dupes == 0, f"{dupes} rows served on two pages"
    assert set(seen) == expected, f"never served: {sorted(expected - set(seen))}"


@pytest.mark.parametrize("sort_by", ["measurement_count", "name"])
@pytest.mark.parametrize("sort_dir", ["desc", "asc"])
async def test_bioactivity_chemicals(
    pg_session: AsyncSession, sort_by: str, sort_dir: str
) -> None:
    rows = [
        {"bn": "tied", "bid": "b1", "cn": f"chem {i % 9}", "cid": f"c{i:03}"}
        for i in range(_N)
    ]
    await pg_session.execute(_CHEM_BIOACT_INSERT, rows)

    seen = await _walk(
        lambda p: bioactivity.get_chemicals(
            pg_session,
            "tied",
            page=p,
            sort_by=sort_by,
            sort_dir=sort_dir,
            rows_per_page=_PER_PAGE,
        )
    )
    _assert_each_once(seen, {r["cid"] for r in rows})


@pytest.mark.parametrize("sort_dir", ["desc", "asc"])
async def test_chemical_bioactivities(pg_session: AsyncSession, sort_dir: str) -> None:
    rows = [
        {"bn": f"bio {i % 9}", "bid": f"b{i:03}", "cn": "tied", "cid": "c1"}
        for i in range(_N)
    ]
    await pg_session.execute(_CHEM_BIOACT_INSERT, rows)

    seen = await _walk(
        lambda p: bioactivity.get_chemical_bioactivities(
            pg_session, "tied", page=p, sort_dir=sort_dir, rows_per_page=_PER_PAGE
        )
    )
    _assert_each_once(seen, {r["bid"] for r in rows})


@pytest.mark.parametrize("sort_by", ["evidence_count", "name"])
async def test_disease_correlation(pg_session: AsyncSession, sort_by: str) -> None:
    rows = [
        {"i": i, "cn": f"chem {i % 9}", "cid": f"c{i:03}", "dn": "d", "did": "d1"}
        for i in range(_N)
    ]
    await pg_session.execute(_CORRELATION_INSERT, rows)

    seen = await _walk(
        lambda p: disease.get_correlation(
            pg_session, "d", page=p, sort_by=sort_by, rows_per_page=_PER_PAGE
        )
    )
    _assert_each_once(seen, {r["cid"] for r in rows})


@pytest.mark.parametrize("sort_by", ["evidence_count", "name"])
async def test_chemical_correlation(pg_session: AsyncSession, sort_by: str) -> None:
    rows = [
        {"i": i, "cn": "c", "cid": "c1", "dn": f"dis {i % 9}", "did": f"d{i:03}"}
        for i in range(_N)
    ]
    await pg_session.execute(_CORRELATION_INSERT, rows)

    seen = await _walk(
        lambda p: chemical.get_correlation(
            pg_session, "c", page=p, sort_by=sort_by, rows_per_page=_PER_PAGE
        )
    )
    _assert_each_once(seen, {r["did"] for r in rows})
