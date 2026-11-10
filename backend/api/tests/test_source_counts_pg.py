"""Assay Source filter reads the stored per-pair counts, not the sample.

The MV ``measurements`` column holds at most 25 measurements per pair. The
filter used to test that sample and rewrite ``measurement_count`` to the
sample's length, so the "N assays" button capped at 25 while the modal
listed every assay (quercetin x anticancer: 25 vs 594). These fixtures put
the deciding measurements past the cap, where only the stored counts see
them.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from src.repositories.bioactivity import get_chemicals, get_source_kind_counts
from tests.dbharness import chem_bioactivity_row, measurement

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BIOACT = "anticancer"
CAP = 25

_EXP_SAMPLE = [measurement(evidence_source="experimental")] * CAP

_ROWS = [
    # 40 experimental assays; the sample shows only the first 25.
    chem_bioactivity_row(
        bioactivity=BIOACT,
        chemical="many-exp",
        chem_id="c1",
        measurements=_EXP_SAMPLE,
        measurement_count=40,
        experimental_count=40,
        predicted_count=0,
    ),
    # 3 predicted assays, all past the cap: the sample is all experimental.
    chem_bioactivity_row(
        bioactivity=BIOACT,
        chemical="pred-past-cap",
        chem_id="c2",
        measurements=_EXP_SAMPLE,
        measurement_count=30,
        experimental_count=27,
        predicted_count=3,
    ),
    # Ranks first by total (50) but third by experimental count (10).
    chem_bioactivity_row(
        bioactivity=BIOACT,
        chemical="mostly-pred",
        chem_id="c4",
        measurements=[measurement(evidence_source="experimental")] * 10
        + [measurement(evidence_source="predicted")] * 15,
        measurement_count=50,
        experimental_count=10,
        predicted_count=40,
    ),
    chem_bioactivity_row(
        bioactivity=BIOACT,
        chemical="few-exp",
        chem_id="c3",
        measurements=[measurement(evidence_source="experimental")] * 5,
    ),
]


async def _load(session: AsyncSession) -> None:
    for r in _ROWS:
        await session.execute(
            text(
                "INSERT INTO mv_chemical_bioactivity (bioactivity_name,"
                " bioactivity_foodatlas_id, chemical_name, chemical_foodatlas_id,"
                " measurement_count, experimental_count, predicted_count,"
                " measurements) VALUES (:bioactivity_name,"
                " :bioactivity_foodatlas_id, :chemical_name,"
                " :chemical_foodatlas_id, :measurement_count,"
                " :experimental_count, :predicted_count,"
                " CAST(:measurements AS JSONB))"
            ),
            {**r, "measurements": json.dumps(r["measurements"])},
        )
    await session.commit()


def _counts(payload: dict) -> dict[str, int]:
    return {r["name"]: r["measurement_count"] for r in payload["data"]}


async def test_count_is_not_capped_at_sample(pg_session: AsyncSession) -> None:
    await _load(pg_session)
    got = await get_chemicals(pg_session, BIOACT, filter_source_kind="experimental")
    assert _counts(got) == {
        "many-exp": 40,
        "pred-past-cap": 27,
        "mostly-pred": 10,
        "few-exp": 5,
    }


async def test_predicted_past_cap_is_found(pg_session: AsyncSession) -> None:
    await _load(pg_session)
    got = await get_chemicals(pg_session, BIOACT, filter_source_kind="predicted")
    assert _counts(got) == {"pred-past-cap": 3, "mostly-pred": 40}


async def test_unfiltered_count_is_total(pg_session: AsyncSession) -> None:
    await _load(pg_session)
    got = await get_chemicals(pg_session, BIOACT)
    assert _counts(got) == {
        "many-exp": 40,
        "pred-past-cap": 30,
        "mostly-pred": 50,
        "few-exp": 5,
    }


async def test_sort_uses_filtered_count(pg_session: AsyncSession) -> None:
    await _load(pg_session)
    got = await get_chemicals(
        pg_session,
        BIOACT,
        sort_by="measurement_count",
        sort_dir="desc",
        filter_source_kind="experimental",
    )
    assert [r["name"] for r in got["data"]] == [
        "many-exp",
        "pred-past-cap",
        "mostly-pred",
        "few-exp",
    ]
    asc = await get_chemicals(
        pg_session,
        BIOACT,
        sort_by="measurement_count",
        sort_dir="asc",
        filter_source_kind="experimental",
    )
    assert [r["name"] for r in asc["data"]] == [
        "few-exp",
        "mostly-pred",
        "pred-past-cap",
        "many-exp",
    ]


async def test_facet_counts_see_past_cap(pg_session: AsyncSession) -> None:
    await _load(pg_session)
    facets = await get_source_kind_counts(pg_session, BIOACT, "bioactivity-chemicals")
    assert facets["data"] == {"both": 4, "experimental": 4, "predicted": 2}


async def test_stored_count_columns_not_leaked(pg_session: AsyncSession) -> None:
    await _load(pg_session)
    for kind in ("", "experimental"):
        got = await get_chemicals(pg_session, BIOACT, filter_source_kind=kind)
        for row in got["data"]:
            assert "experimental_count" not in row
            assert "predicted_count" not in row
