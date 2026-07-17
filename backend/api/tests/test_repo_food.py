"""Tests for food repository query functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.food import (
    _resort_after_filter,
    get_composition,
    get_composition_counts,
    get_metadata,
    get_profile,
)


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
            chemical_classification=["carbohydrate"],
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
            chemical_classification=["unknown category"],
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
            chemical_classification=None,
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
            chemical_classification=["carbohydrate"],
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


class TestResortAfterFilter:
    def test_evidence_count_desc(self) -> None:
        rows = [
            {"name": "a", "fdc_evidences": [1, 2], "foodatlas_evidences": [3]},
            {
                "name": "b",
                "fdc_evidences": [1],
                "foodatlas_evidences": [2, 3],
                "dmd_evidences": [4],
            },
            {"name": "c", "fdc_evidences": []},
        ]
        out = _resort_after_filter(rows, "evidence_count", "DESC")
        assert [r["name"] for r in out] == ["b", "a", "c"]

    def test_evidence_count_asc(self) -> None:
        rows = [
            {"name": "a", "fdc_evidences": [1, 2]},
            {"name": "b", "fdc_evidences": [1]},
        ]
        out = _resort_after_filter(rows, "evidence_count", "ASC")
        assert [r["name"] for r in out] == ["b", "a"]

    def test_null_evidences_treated_as_empty(self) -> None:
        rows = [
            {"name": "a", "fdc_evidences": None, "foodatlas_evidences": None},
            {"name": "b", "fdc_evidences": [1]},
        ]
        out = _resort_after_filter(rows, "evidence_count", "DESC")
        assert [r["name"] for r in out] == ["b", "a"]


def _mock_session_sequence(*result_rows: list[MagicMock]) -> AsyncMock:
    """Session where each execute() returns the next batch of rows.

    Used by get_composition_counts which fetches composition rows first
    then the trust scores in a second query.
    """
    session = AsyncMock()
    results = []
    for rows in result_rows:
        r = MagicMock()
        r.__iter__ = lambda self, _rows=rows: iter(_rows)
        # `for row in result` iterates; `_fetch_trust_scores` uses that too.
        results.append(r)
    session.execute.side_effect = results
    return session


class TestFoodGetCompositionCounts:
    @pytest.mark.asyncio
    async def test_faceted_counts_shape(self) -> None:
        rows = [
            _make_row(
                id=1,
                chemical_name="glucose",
                chemical_classification=["carbohydrate"],
                median_concentration={"value": 5.0},
                fdc_evidences=[{"extraction": [{"attestation_id": "a1"}]}],
                foodatlas_evidences=None,
            ),
            _make_row(
                id=2,
                chemical_name="quercetin",
                chemical_classification=["flavonoid"],
                median_concentration=None,  # no-concentration row
                fdc_evidences=None,
                foodatlas_evidences=[{"extraction": [{"attestation_id": "a2"}]}],
            ),
        ]
        # Second execute() (via _fetch_trust_scores) returns no scores →
        # neither row is fully-low-trust.
        session = _mock_session_sequence(rows, [])
        out = await get_composition_counts(session, "apple")
        data = out["data"]
        # Composition rows: 1 carbohydrate + 1 flavonoid + 2 total sources.
        assert data["classification_counts"] == {
            "carbohydrate": 1,
            "flavonoid": 1,
        }
        assert data["source_counts"] == {"fdc": 1, "foodatlas": 1}
        assert data["no_concentration_count"] == 1
        assert data["low_trust_count"] == 0

    @pytest.mark.asyncio
    async def test_filters_narrow_counts(self) -> None:
        """Selecting a classification narrows the source_counts + no-conc
        + low-trust counts to only rows in that class (facet-excluding-self
        semantics for the other dimensions)."""
        rows = [
            _make_row(
                id=1,
                chemical_name="glucose",
                chemical_classification=["carbohydrate"],
                median_concentration={"value": 5.0},
                fdc_evidences=[{"extraction": [{"attestation_id": "a1"}]}],
                foodatlas_evidences=None,
            ),
            _make_row(
                id=2,
                chemical_name="quercetin",
                chemical_classification=["flavonoid"],
                median_concentration=None,
                fdc_evidences=None,
                foodatlas_evidences=[{"extraction": [{"attestation_id": "a2"}]}],
            ),
        ]
        session = _mock_session_sequence(rows, [])
        out = await get_composition_counts(
            session, "apple", filter_classification="carbohydrate"
        )
        data = out["data"]
        # source + no-conc excludes flavonoid rows once class filter is on.
        assert data["source_counts"] == {"fdc": 1, "foodatlas": 0}
        assert data["no_concentration_count"] == 0
        # classification_counts is faceted (excludes class dim) — still
        # shows both classifications.
        assert data["classification_counts"] == {
            "carbohydrate": 1,
            "flavonoid": 1,
        }

    @pytest.mark.asyncio
    async def test_search_narrows_all(self) -> None:
        rows = [
            _make_row(
                id=1,
                chemical_name="glucose",
                chemical_classification=["carbohydrate"],
                median_concentration={"value": 5.0},
                fdc_evidences=[{"extraction": []}],
                foodatlas_evidences=None,
            ),
            _make_row(
                id=2,
                chemical_name="quercetin",
                chemical_classification=["flavonoid"],
                median_concentration=None,
                fdc_evidences=None,
                foodatlas_evidences=[{"extraction": []}],
            ),
        ]
        session = _mock_session_sequence(rows, [])
        out = await get_composition_counts(session, "apple", search_term="quer")
        data = out["data"]
        # Only quercetin passes search → no carbohydrate, one flavonoid.
        assert data["classification_counts"] == {"flavonoid": 1}
        assert data["source_counts"] == {"fdc": 0, "foodatlas": 1}
