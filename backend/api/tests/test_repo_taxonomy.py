"""Tests for taxonomy repository functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.taxonomy import get_taxonomy


def _make_row(**kwargs: object) -> MagicMock:
    row = MagicMock()
    row._mapping = kwargs
    return row


def _mock_resolve_result(entity_id: str | None) -> MagicMock:
    """Mock the result from _resolve_entity_id."""
    result = MagicMock()
    if entity_id is None:
        result.first.return_value = None
    else:
        result.first.return_value = (entity_id,)
    return result


def _mock_iter_result(rows: list[MagicMock]) -> MagicMock:
    result = MagicMock()
    result.__iter__ = lambda self: iter(rows)
    return result


def _mock_page_ids_result(ids: list[str]) -> MagicMock:
    result = MagicMock()
    result.__iter__ = lambda self: iter([(i,) for i in ids])
    return result


class TestGetTaxonomy:
    @pytest.mark.asyncio
    async def test_linear_ancestry(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [
            _mock_resolve_result("e123"),
            _mock_iter_result(
                [
                    _make_row(
                        node_id="e123",
                        node_name="apple",
                        came_from_id=None,
                        came_from_name=None,
                    ),
                    _make_row(
                        node_id="e59",
                        node_name="plant fruit food product",
                        came_from_id="e123",
                        came_from_name="apple",
                    ),
                    _make_row(
                        node_id="e19",
                        node_name="plant food product",
                        came_from_id="e59",
                        came_from_name="plant fruit food product",
                    ),
                ]
            ),
            _mock_page_ids_result(["e123", "e59"]),
        ]
        result = await get_taxonomy(session, "apple", "food")
        data = result["data"]
        assert data["entity_id"] == "e123"
        assert len(data["nodes"]) == 3
        assert len(data["edges"]) == 2
        page_map = {n["id"]: n["has_page"] for n in data["nodes"]}
        assert page_map["e123"] is True
        assert page_map["e19"] is False

    @pytest.mark.asyncio
    async def test_dag_multiple_parents(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [
            _mock_resolve_result("e100"),
            _mock_iter_result(
                [
                    _make_row(
                        node_id="e100",
                        node_name="quercetin",
                        came_from_id=None,
                        came_from_name=None,
                    ),
                    _make_row(
                        node_id="e200",
                        node_name="flavonol",
                        came_from_id="e100",
                        came_from_name="quercetin",
                    ),
                    _make_row(
                        node_id="e300",
                        node_name="polyphenol",
                        came_from_id="e100",
                        came_from_name="quercetin",
                    ),
                ]
            ),
            _mock_page_ids_result(["e100"]),
        ]
        result = await get_taxonomy(session, "quercetin", "chemical")
        data = result["data"]
        assert len(data["edges"]) == 2
        edge_parents = {e["parent_id"] for e in data["edges"]}
        assert edge_parents == {"e200", "e300"}
        assert all(e["child_id"] == "e100" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_no_ancestors(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [
            _mock_resolve_result("e999"),
            _mock_iter_result(
                [
                    _make_row(
                        node_id="e999",
                        node_name="isolated",
                        came_from_id=None,
                        came_from_name=None,
                    ),
                ]
            ),
            _mock_page_ids_result(["e999"]),
        ]
        result = await get_taxonomy(session, "isolated", "disease")
        data = result["data"]
        assert data["entity_id"] == "e999"
        assert len(data["nodes"]) == 1
        assert len(data["edges"]) == 0

    @pytest.mark.asyncio
    async def test_unknown_entity(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_mock_resolve_result(None)]
        result = await get_taxonomy(session, "nonexistent", "food")
        assert result["data"]["entity_id"] is None
        assert result["data"]["nodes"] == []
        assert result["data"]["edges"] == []

    @pytest.mark.asyncio
    async def test_deduplicates_nodes(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [
            _mock_resolve_result("e1"),
            _mock_iter_result(
                [
                    _make_row(
                        node_id="e1",
                        node_name="child",
                        came_from_id=None,
                        came_from_name=None,
                    ),
                    _make_row(
                        node_id="e2",
                        node_name="parent_a",
                        came_from_id="e1",
                        came_from_name="child",
                    ),
                    _make_row(
                        node_id="e3",
                        node_name="parent_b",
                        came_from_id="e1",
                        came_from_name="child",
                    ),
                    _make_row(
                        node_id="e4",
                        node_name="grandparent",
                        came_from_id="e2",
                        came_from_name="parent_a",
                    ),
                    _make_row(
                        node_id="e4",
                        node_name="grandparent",
                        came_from_id="e3",
                        came_from_name="parent_b",
                    ),
                ]
            ),
            _mock_page_ids_result(["e1"]),
        ]
        result = await get_taxonomy(session, "child", "food")
        data = result["data"]
        node_ids = [n["id"] for n in data["nodes"]]
        assert len(node_ids) == len(set(node_ids))
        assert len(data["edges"]) == 4
