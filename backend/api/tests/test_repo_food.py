"""Tests for food repository query functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.food import get_composition, get_metadata, get_profile


def _make_row(**kwargs: object) -> MagicMock:
    """Create a mock row with _mapping attribute."""
    row = MagicMock()
    row._mapping = kwargs
    return row


def _mock_session_single(rows: list[MagicMock]) -> AsyncMock:
    """Session that returns rows for one execute call."""
    session = AsyncMock()
    result = MagicMock()
    result.__iter__ = lambda self: iter(rows)
    session.execute.return_value = result
    return session


class TestFoodGetMetadata:
    @pytest.mark.asyncio
    async def test_returns_data_and_metadata(self) -> None:
        row = _make_row(
            common_name="apple",
            id="FA:0001",
            entity_type="food",
            scientific_name="Malus domestica",
            synonyms=[],
            external_ids={},
            food_classification="fruit",
        )
        session = _mock_session_single([row])
        result = await get_metadata(session, "apple")
        assert result["metadata"]["row_count"] == 1
        assert result["data"][0]["common_name"] == "apple"

    @pytest.mark.asyncio
    async def test_empty_result(self) -> None:
        session = _mock_session_single([])
        result = await get_metadata(session, "nonexistent")
        assert result["data"] == []
        assert result["metadata"]["row_count"] == 0


class TestFoodGetProfile:
    @pytest.mark.asyncio
    async def test_groups_by_classification(self) -> None:
        row = _make_row(
            name="glucose",
            id="FA:C001",
            nutrient_classification=["carbohydrate (including fiber)"],
            median_concentration=5.0,
        )
        session = _mock_session_single([row])
        result = await get_profile(session, "apple")
        carbs = result["data"]["carbohydrates(incl.fiber)"]
        assert len(carbs) == 1
        assert carbs[0]["name"] == "glucose"

    @pytest.mark.asyncio
    async def test_unknown_classification_ignored(self) -> None:
        row = _make_row(
            name="mystery",
            id="FA:C999",
            nutrient_classification=["unknown category"],
            median_concentration=1.0,
        )
        session = _mock_session_single([row])
        result = await get_profile(session, "apple")
        for bucket in result["data"].values():
            assert len(bucket) == 0

    @pytest.mark.asyncio
    async def test_null_classification_handled(self) -> None:
        row = _make_row(
            name="water",
            id="FA:C002",
            nutrient_classification=None,
            median_concentration=100.0,
        )
        session = _mock_session_single([row])
        result = await get_profile(session, "apple")
        # None classification -> no buckets filled
        total = sum(len(v) for v in result["data"].values())
        assert total == 0


class TestFoodGetComposition:
    @pytest.mark.asyncio
    async def test_returns_paginated_structure(self) -> None:
        row = _make_row(
            name="glucose",
            id="FA:C001",
            nutrient_classification=["carbohydrate (including fiber)"],
            median_concentration=5.0,
        )
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([row])
        count_result = MagicMock()
        count_result.scalar.return_value = 1
        session.execute.side_effect = [data_result, count_result]

        result = await get_composition(session, "apple")
        assert "data" in result
        assert result["metadata"]["total_rows"] == 1
        assert result["metadata"]["rows_per_page"] == 25

    @pytest.mark.asyncio
    async def test_empty_filter_source_returns_empty(self) -> None:
        # filter_source="" with sources splitting to empty list -> normal query
        # But filter_source with no valid sources after split -> empty
        result = await get_composition(AsyncMock(), "apple", filter_source="+")
        assert result["data"] == []
        assert result["metadata"]["row_count"] == 0

    @pytest.mark.asyncio
    async def test_search_and_sort_params(self) -> None:
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([])
        count_result = MagicMock()
        count_result.scalar.return_value = 0
        session.execute.side_effect = [data_result, count_result]

        result = await get_composition(
            session,
            "apple",
            page=2,
            filter_source="fdc",
            search_term="glu",
            sort_by="median_concentration",
            sort_dir="asc",
            show_all_rows=False,
        )
        assert result["metadata"]["current_page"] == 2

    @pytest.mark.asyncio
    async def test_multiple_sources(self) -> None:
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([])
        count_result = MagicMock()
        count_result.scalar.return_value = 0
        session.execute.side_effect = [data_result, count_result]

        result = await get_composition(session, "apple", filter_source="fdc+dmd")
        assert result["metadata"]["total_pages"] == 0
