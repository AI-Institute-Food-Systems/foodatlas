"""Tests for the /bioactivity/evidence_types sidebar-count endpoint
and its repository function `get_evidence_type_counts`.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from src.repositories.bioactivity import get_evidence_type_counts


def _make_row(**kwargs: object) -> MagicMock:
    row = MagicMock()
    row._mapping = kwargs
    return row


def _mock_session(rows: list[MagicMock]) -> AsyncMock:
    session = AsyncMock()
    result = MagicMock()
    result.__iter__ = lambda self: iter(rows)
    session.execute.return_value = result
    return session


# -- Route -----------------------------------------------------------------


SAMPLE_PAYLOAD = {
    "data": [
        {"evidence_type": "in vitro", "count": 42},
        {"evidence_type": "in vivo", "count": 7},
        {"evidence_type": "molecular-level", "count": 3},
    ],
    "metadata": {"row_count": 3},
}


class TestEvidenceTypesRoute:
    def test_returns_data_and_metadata(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        del mock_db  # fixture must be requested to override the DB dep
        with patch(
            "src.repositories.bioactivity.get_evidence_type_counts",
            return_value=SAMPLE_PAYLOAD,
        ):
            resp = client.get(
                "/bioactivity/evidence_types",
                params={
                    "common_name": "antioxidant",
                    "direction": "bioactivity-chemicals",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["metadata"]["row_count"] == 3
        assert body["data"][0]["evidence_type"] == "in vitro"

    def test_missing_common_name_returns_422(self, client: TestClient) -> None:
        resp = client.get(
            "/bioactivity/evidence_types",
            params={"direction": "bioactivity-chemicals"},
        )
        assert resp.status_code == 422

    def test_missing_direction_returns_422(self, client: TestClient) -> None:
        resp = client.get(
            "/bioactivity/evidence_types",
            params={"common_name": "antioxidant"},
        )
        assert resp.status_code == 422


# -- Repository ------------------------------------------------------------


class TestGetEvidenceTypeCounts:
    @pytest.mark.asyncio
    async def test_returns_rows_sorted_by_count(self) -> None:
        rows = [
            _make_row(evidence_type="in vitro", count=42),
            _make_row(evidence_type="in vivo", count=7),
        ]
        session = _mock_session(rows)
        result = await get_evidence_type_counts(
            session, "antioxidant", "bioactivity-chemicals"
        )
        assert result["metadata"]["row_count"] == 2
        assert result["data"] == [
            {"evidence_type": "in vitro", "count": 42},
            {"evidence_type": "in vivo", "count": 7},
        ]

    @pytest.mark.asyncio
    async def test_empty_result(self) -> None:
        session = _mock_session([])
        result = await get_evidence_type_counts(
            session, "nonexistent", "bioactivity-chemicals"
        )
        assert result["data"] == []
        assert result["metadata"]["row_count"] == 0

    @pytest.mark.asyncio
    async def test_unknown_direction_returns_empty(self) -> None:
        session = AsyncMock()
        result = await get_evidence_type_counts(
            session, "antioxidant", "not-a-real-direction"
        )
        assert result["data"] == []
        assert result["metadata"]["row_count"] == 0
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_food_inferred_direction_uses_join_query(self) -> None:
        rows = [_make_row(evidence_type="in vitro", count=5)]
        session = _mock_session(rows)
        await get_evidence_type_counts(session, "apple", "food-inferred-bioactivities")
        session.execute.assert_called_once()
        sql = str(session.execute.call_args[0][0])
        assert "mv_food_chemical_composition" in sql
        assert "mv_chemical_bioactivity" in sql

    @pytest.mark.asyncio
    async def test_regular_direction_uses_single_mv(self) -> None:
        rows = [_make_row(evidence_type="in vitro", count=10)]
        session = _mock_session(rows)
        await get_evidence_type_counts(session, "apple", "food-bioactivities")
        session.execute.assert_called_once()
        sql = str(session.execute.call_args[0][0])
        assert "mv_food_bioactivity" in sql
        assert "food_name" in sql
