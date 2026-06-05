"""Tests for src.repositories.bioactivity (internal SSR repository)."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.bioactivity import (
    get_chemicals,
    get_diseases,
    get_foods,
    get_metadata,
)


def _make_row(**kwargs: object) -> MagicMock:
    row = MagicMock()
    row._mapping = kwargs
    return row


def _iter_result(rows: list[MagicMock]) -> MagicMock:
    result = MagicMock()
    result.__iter__ = lambda self: iter(rows)
    return result


def _scalar_result(value: int) -> MagicMock:
    result = MagicMock()
    result.scalar.return_value = value
    return result


class TestGetMetadata:
    @pytest.mark.asyncio
    async def test_returns_data_and_count(self) -> None:
        row = _make_row(
            common_name="anti-inflammatory",
            id="bio1",
            entity_type="bioactivity",
            scientific_name="",
            synonyms=[],
            external_ids={},
            description="reduces inflammation",
            ambiguity_siblings=[],
        )
        session = AsyncMock()
        session.execute.return_value = _iter_result([row])

        result = await get_metadata(session, "anti-inflammatory")

        assert result["metadata"]["row_count"] == 1
        assert result["data"][0]["description"] == "reduces inflammation"


class TestGetChemicals:
    @pytest.mark.asyncio
    async def test_returns_paginated(self) -> None:
        row = _make_row(id="c1", name="quercetin", measurement_count=2, measurements=[])
        session = AsyncMock()
        session.execute.side_effect = [_iter_result([row]), _scalar_result(1)]

        result = await get_chemicals(session, "anti-inflammatory", page=1)

        assert result["data"][0]["name"] == "quercetin"
        assert result["metadata"]["total_rows"] == 1
        assert result["metadata"]["total_pages"] == 1


class TestGetFoods:
    @pytest.mark.asyncio
    async def test_filters_by_exhibit_type(self) -> None:
        row = _make_row(
            id="f1",
            name="strawberry",
            exhibit_type="direct",
            via_chemical_id=None,
            via_chemical_name=None,
            efficacy_pred=None,
            evidence_count=1,
            evidences=[],
        )
        session = AsyncMock()
        session.execute.side_effect = [_iter_result([row]), _scalar_result(1)]

        result = await get_foods(session, "antioxidant", exhibit_type="direct")

        # The list query should carry the et param.
        list_params = session.execute.call_args_list[0][0][1]
        assert list_params["et"] == "direct"
        assert result["data"][0]["exhibit_type"] == "direct"

    @pytest.mark.asyncio
    async def test_all_exhibit_type_skips_filter(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_iter_result([]), _scalar_result(0)]
        await get_foods(session, "antioxidant", exhibit_type="all")
        list_params = session.execute.call_args_list[0][0][1]
        assert "et" not in list_params


class TestGetDiseases:
    @pytest.mark.asyncio
    async def test_returns_targets(self) -> None:
        row = _make_row(
            id="d1",
            name="asthma",
            polarity=None,
            target_ids=["UniProt:P1"],
            evidence_count=1,
            evidences=[],
        )
        session = AsyncMock()
        session.execute.side_effect = [_iter_result([row]), _scalar_result(1)]

        result = await get_diseases(session, "anti-inflammatory")

        assert result["data"][0]["target_ids"] == ["UniProt:P1"]
        assert result["metadata"]["total_rows"] == 1
