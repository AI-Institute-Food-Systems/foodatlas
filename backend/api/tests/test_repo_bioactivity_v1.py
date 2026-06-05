"""Tests for src.repositories.v1.bioactivity (3 MV-backed list endpoints)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.v1 import bioactivity


def _row(**kwargs: object) -> MagicMock:
    r = MagicMock()
    r._mapping = kwargs
    for key, val in kwargs.items():
        setattr(r, key, val)
    return r


def _iter_result(rows: list[MagicMock]) -> MagicMock:
    result = MagicMock()
    result.__iter__ = lambda self: iter(rows)
    return result


def _scalar_result(value: int) -> MagicMock:
    result = MagicMock()
    result.scalar.return_value = value
    return result


class TestListMeasurements:
    @pytest.mark.asyncio
    async def test_filter_by_chemical_id(self) -> None:
        row = _row(
            chemical_id="c1",
            chemical_name="quercetin",
            bioactivity_id="bio1",
            bioactivity_name="anti-inflammatory",
            measurement_count=2,
            potency={"value": 5.0, "unit": "uM"},
            hill_curve={
                "zero_activity": 0.5,
                "infinite_activity": -50.0,
                "log_ac50": -5.0,
                "hill_slope": 1.0,
            },
            target_ids=["UniProt:P1"],
            evidence_source="PubChem AID:1",
            evidence_type="In vitro",
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await bioactivity.list_measurements(session, chemical_id="c1")
        assert total == 1
        assert rows[0]["bioactivity_name"] == "anti-inflammatory"

    @pytest.mark.asyncio
    async def test_filter_by_bioactivity_id(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        rows, total = await bioactivity.list_measurements(
            session, bioactivity_id="bio1"
        )
        assert total == 0
        assert rows == []

    @pytest.mark.asyncio
    async def test_no_filter_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await bioactivity.list_measurements(session)
        assert rows == [] and total == 0
        session.execute.assert_not_called()


class TestListExhibits:
    @pytest.mark.asyncio
    async def test_filter_by_food_id(self) -> None:
        row = _row(
            food_id="f1",
            food_name="strawberry",
            bioactivity_id="bio1",
            bioactivity_name="antioxidant",
            exhibit_type="direct",
            via_chemical_id=None,
            via_chemical_name=None,
            efficacy_pred=None,
            evidence_count=1,
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await bioactivity.list_exhibits(session, food_id="f1")
        assert total == 1
        assert rows[0]["exhibit_type"] == "direct"

    @pytest.mark.asyncio
    async def test_filter_exhibit_type_direct(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        await bioactivity.list_exhibits(session, food_id="f1", exhibit_type="direct")
        # First execute is the count query; verify ``et`` param was bound.
        count_params = session.execute.call_args_list[0][0][1]
        assert count_params["et"] == "direct"

    @pytest.mark.asyncio
    async def test_invalid_exhibit_type_falls_through_to_all(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        await bioactivity.list_exhibits(session, food_id="f1", exhibit_type="bogus")
        count_params = session.execute.call_args_list[0][0][1]
        # 'bogus' is not in {direct, inherited}; no et filter applied.
        assert "et" not in count_params

    @pytest.mark.asyncio
    async def test_no_filter_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await bioactivity.list_exhibits(session)
        assert rows == [] and total == 0
        session.execute.assert_not_called()


class TestListAssociations:
    @pytest.mark.asyncio
    async def test_filter_by_bioactivity_id(self) -> None:
        row = _row(
            bioactivity_id="bio1",
            bioactivity_name="anti-inflammatory",
            disease_id="d1",
            disease_name="asthma",
            polarity=None,
            target_ids=["UniProt:P1"],
            evidence_count=1,
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await bioactivity.list_associations(
            session, bioactivity_id="bio1"
        )
        assert total == 1
        assert rows[0]["disease_name"] == "asthma"

    @pytest.mark.asyncio
    async def test_filter_by_disease_id(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        rows, total = await bioactivity.list_associations(session, disease_id="d1")
        assert total == 0
        assert rows == []

    @pytest.mark.asyncio
    async def test_no_filter_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await bioactivity.list_associations(session)
        assert rows == [] and total == 0
        session.execute.assert_not_called()
