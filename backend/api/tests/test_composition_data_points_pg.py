"""A composition data point is one extraction, on every composition surface.

The evidence modal lists extractions, so the chemical page's per-source
counts, the "N data points" button and the food page's data-point sort must
count extractions too. PTFI records often carry several extractions, which
is where counting records disagreed with the modal.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from src.repositories import chemical, food

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _evs(source: str, *extractions: int) -> list[dict]:
    """One record per argument, each with that many extractions."""
    return [
        {
            "reference": {"source_name": source},
            "extraction": [{"extracted_chemical_name": "q"}] * n,
        }
        for n in extractions
    ]


# food → (fdc, foodatlas, ptfi) evidence lists for chemical "quercetin"
ROWS = {
    "apple": (_evs("FDC", 1), _evs("FoodAtlas", 2), _evs("PTFI", 1, 3)),
    "pear": (_evs("FDC", 1, 1, 1), None, None),
    "plum": (None, None, _evs("PTFI", 5)),
}


async def _load(session: AsyncSession) -> None:
    for i, (name, (fdc, fa, ptfi)) in enumerate(ROWS.items()):
        await session.execute(
            text(
                "INSERT INTO mv_food_chemical_composition (id, food_name,"
                " food_foodatlas_id, chemical_name, chemical_foodatlas_id,"
                " chemical_classification, median_concentration, fdc_evidences,"
                " foodatlas_evidences, ptfi_evidences) VALUES (:i, :f, :fid,"
                " 'quercetin', 'c1', '{}', NULL, CAST(:fdc AS JSONB),"
                " CAST(:fa AS JSONB), CAST(:ptfi AS JSONB))"
            ),
            {
                "i": i,
                "f": name,
                "fid": f"f{i}",
                "fdc": json.dumps(fdc) if fdc else None,
                "fa": json.dumps(fa) if fa else None,
                "ptfi": json.dumps(ptfi) if ptfi else None,
            },
        )
    await session.commit()


def _extractions(evs: list[dict] | None) -> int:
    return sum(len(ev["extraction"]) for ev in evs or [])


async def test_chemical_counts_equal_modal_extractions(
    pg_session: AsyncSession,
) -> None:
    await _load(pg_session)
    payload = await chemical.get_composition(pg_session, "quercetin")
    data = payload["data"]
    assert isinstance(data, dict)
    rows = {r["name"]: r for r in data["without_concentrations"]}
    assert set(rows) == set(ROWS)
    for name, (fdc, fa, ptfi) in ROWS.items():
        row = rows[name]
        assert row["fdc_count"] == _extractions(fdc)
        assert row["foodatlas_count"] == _extractions(fa)
        assert row["ptfi_count"] == _extractions(ptfi)
        evidence = await chemical.get_composition_evidence(
            pg_session, "quercetin", name
        )
        evs = evidence["data"]
        assert isinstance(evs, list)
        assert row["evidence_count"] == _extractions(evs)


async def test_food_sort_counts_extractions(pg_session: AsyncSession) -> None:
    """plum has one record with 5 extractions; pear has 3 single ones."""
    await _load(pg_session)
    for name in ROWS:
        await pg_session.execute(
            text(
                "UPDATE mv_food_chemical_composition SET food_name = 'mix',"
                " chemical_name = :c WHERE food_name = :f"
            ),
            {"c": name, "f": name},
        )
    await pg_session.commit()
    payload = await food.get_composition(
        pg_session, "mix", sort_by="evidence_count", sort_dir="desc", rows_per_page=10
    )
    data = payload["data"]
    assert isinstance(data, list)
    assert [r["name"] for r in data] == ["apple", "plum", "pear"]
