"""Tests for the entity index behind /metadata/entities (the sitemap)."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.entity_index import list_entities


def _session(rows: list[dict[str, str]]) -> AsyncMock:
    mapped = []
    for r in rows:
        row = MagicMock()
        row._mapping = r
        mapped.append(row)
    session = AsyncMock()
    result = MagicMock()
    result.__iter__ = lambda self: iter(mapped)
    session.execute.return_value = result
    return session


def _sql(session: AsyncMock) -> str:
    return str(session.execute.call_args.args[0])


class TestListEntities:
    @pytest.mark.asyncio
    async def test_returns_every_row_as_plain_dicts(self) -> None:
        rows = [
            {"foodatlas_id": "e1", "entity_type": "chemical", "common_name": "q"},
            {"foodatlas_id": "e2", "entity_type": "food", "common_name": "tomato"},
        ]
        assert await list_entities(_session(rows)) == rows

    @pytest.mark.asyncio
    async def test_reads_every_source_that_gives_a_page(self) -> None:
        session = _session([])
        await list_entities(session)
        sql = _sql(session)
        for table in (
            "mv_food_entities",
            "mv_chemical_entities",
            "mv_chemical_bioactivity",
            "mv_disease_entities",
            "mv_chemical_disease_bioactivity",
            "mv_bioactivity_entities",
        ):
            assert table in sql
        # Not the search MV: it omits pages that exist.
        assert "mv_search_auto_complete" not in sql

    @pytest.mark.asyncio
    async def test_one_row_per_name_and_metadata_rows_first(self) -> None:
        session = _session([])
        await list_entities(session)
        sql = _sql(session)
        assert "DISTINCT ON (entity_type, common_name)" in sql
        assert "ORDER BY entity_type, tier, common_name" in sql

    @pytest.mark.asyncio
    async def test_type_filter_reads_only_that_type(self) -> None:
        session = _session([])
        await list_entities(session, "chemical")
        sql = _sql(session)
        assert "mv_chemical_entities" in sql
        assert "mv_chemical_bioactivity" in sql
        assert "mv_food_entities" not in sql
        assert "mv_disease_entities" not in sql

    @pytest.mark.asyncio
    async def test_food_and_bioactivity_have_no_exception_source(self) -> None:
        session = _session([])
        await list_entities(session, "food")
        assert "UNION ALL" not in _sql(session)
