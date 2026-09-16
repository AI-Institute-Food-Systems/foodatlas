"""The two CTD correlation directions, executed against real Postgres.

The chemical page keeps the source chemical in its row key ("Via
Chemical"); the disease page has no such column and must collapse a
class reached through several descendants into one row with merged
evidence. Both groupings are SQL, so the mock-session tests can only
check that the right GROUP BY text was emitted — this file checks what
comes back.

Fixture, small enough to hold in your head:

  inflammation ← aromatic compound  via resveratrol  r4  papers 7, 8
  inflammation ← aromatic compound  via curcumin     r4  papers 7, 9
  inflammation ← aromatic compound  via curcumin     r3  paper  9
  inflammation ← curcumin           via curcumin     r4  paper  9
  arthritis    ← aromatic compound  via curcumin     r4  paper  5
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from src.repositories import chemical, disease

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.asyncio


# chemical | chem id | rel | disease | disease id | source | source id | pmids
_FIXTURE = """
aromatic compound | eC | r4 | inflammation | eD1 | resveratrol | eR | 7,8
aromatic compound | eC | r4 | inflammation | eD1 | curcumin    | eK | 7,9
aromatic compound | eC | r3 | inflammation | eD1 | curcumin    | eK | 9
curcumin          | eK | r4 | inflammation | eD1 | curcumin    | eK | 9
aromatic compound | eC | r4 | arthritis    | eD2 | curcumin    | eK | 5
"""

_INSERT = text(
    "INSERT INTO mv_chemical_disease_correlation "
    "(id, chemical_name, chemical_foodatlas_id, relationship_id, "
    " disease_name, disease_foodatlas_id, source_chemical_name, "
    " source_chemical_foodatlas_id, evidences, evidence_count) "
    "VALUES (:i, :c, :cid, :r, :d, :did, :s, :sid, CAST(:ev AS jsonb), :n)"
)


async def _load(session: AsyncSession) -> None:
    for i, line in enumerate(_FIXTURE.strip().splitlines()):
        c, cid, r, d, did, s, sid, pmids = (f.strip() for f in line.split("|"))
        ids = pmids.split(",")
        await session.execute(
            _INSERT,
            {
                "i": i,
                "c": c,
                "cid": cid,
                "r": r,
                "d": d,
                "did": did,
                "s": s,
                "sid": sid,
                "ev": json.dumps([{"pmid": {"id": p}} for p in ids]),
                "n": len(ids),
            },
        )
    for cid, name in (
        ("eC", "aromatic compound"),
        ("eK", "curcumin"),
        ("eR", "resveratrol"),
    ):
        await session.execute(
            text(
                "INSERT INTO mv_chemical_entities (foodatlas_id, common_name) "
                "VALUES (:id, :name)"
            ),
            {"id": cid, "name": name},
        )


def _pmids(evidences: list[dict] | None) -> list[str]:
    return sorted(e["pmid"]["id"] for e in evidences or [])


class TestDiseaseSideOneRowPerChemical:
    async def test_class_reached_via_two_descendants_is_one_row(
        self, pg_session: AsyncSession
    ) -> None:
        await _load(pg_session)
        page = await disease.get_correlation(pg_session, "inflammation")
        names = [r["name"] for r in page["data"]["associations"]]
        assert names == ["aromatic compound", "curcumin"]
        assert page["metadata"]["total_rows"] == 2

    async def test_evidence_is_merged_across_sources_and_deduped(
        self, pg_session: AsyncSession
    ) -> None:
        await _load(pg_session)
        page = await disease.get_correlation(pg_session, "inflammation")
        row = page["data"]["associations"][0]
        assert row["name"] == "aromatic compound"
        # Paper 7 arrives via resveratrol AND curcumin: once.
        assert _pmids(row["improves_evidences"]) == ["7", "8", "9"]
        assert _pmids(row["worsens_evidences"]) == ["9"]
        # Paper 9 is cited for both directions: once in the union.
        assert _pmids(row["evidences"]) == ["7", "8", "9"]
        assert sorted(row["relationship_ids"]) == ["r3", "r4"]

    async def test_single_direction_row_keeps_none_for_the_other(
        self, pg_session: AsyncSession
    ) -> None:
        await _load(pg_session)
        page = await disease.get_correlation(pg_session, "inflammation")
        curcumin = page["data"]["associations"][1]
        assert curcumin["worsens_evidences"] is None
        assert _pmids(curcumin["improves_evidences"]) == ["9"]

    async def test_direction_counts_match_the_rendered_rows(
        self, pg_session: AsyncSession
    ) -> None:
        await _load(pg_session)
        counts = await disease.get_correlation_direction_counts(
            pg_session, "inflammation"
        )
        # Two rows render; both improve, one also worsens.
        assert counts == {"improves": 2, "worsens": 1, "both": 2}

    async def test_direction_filter_still_narrows(
        self, pg_session: AsyncSession
    ) -> None:
        await _load(pg_session)
        page = await disease.get_correlation(
            pg_session, "inflammation", relation="negative"
        )
        assert [r["name"] for r in page["data"]["associations"]] == [
            "aromatic compound"
        ]
        assert page["metadata"]["total_rows"] == 1


class TestChemicalSideKeepsAttribution:
    async def test_one_row_per_disease_and_source(
        self, pg_session: AsyncSession
    ) -> None:
        await _load(pg_session)
        page = await chemical.get_correlation(pg_session, "aromatic compound")
        rows = page["data"]["associations"]
        keys = sorted((r["name"], r["source_chemical_name"]) for r in rows)
        assert keys == [
            ("arthritis", "curcumin"),
            ("inflammation", "curcumin"),
            ("inflammation", "resveratrol"),
        ]
        assert page["metadata"]["total_rows"] == 3

    async def test_direction_counts_count_pairs(self, pg_session: AsyncSession) -> None:
        await _load(pg_session)
        counts = await chemical.get_correlation_direction_counts(
            pg_session, "aromatic compound"
        )
        assert counts == {"improves": 3, "worsens": 1, "both": 3}
