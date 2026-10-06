"""Tests for chemical, disease, and search repository functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.chemical import get_composition as chem_composition
from src.repositories.chemical import (
    get_composition_evidence as chem_composition_evidence,
)
from src.repositories.chemical import get_correlation as chem_correlation
from src.repositories.chemical import get_metadata as chem_metadata
from src.repositories.disease import get_correlation as disease_correlation
from src.repositories.disease import get_metadata as disease_metadata
from src.repositories.search import get_statistics, search


def _make_row(**kwargs: object) -> MagicMock:
    row = MagicMock()
    row._mapping = kwargs
    return row


def _mock_session_single(rows: list[MagicMock]) -> AsyncMock:
    session = AsyncMock()
    result = MagicMock()
    result.__iter__ = lambda self: iter(rows)
    session.execute.return_value = result
    return session


# -- Chemical ---------------------------------------------------------------


class TestChemicalGetMetadata:
    @pytest.mark.asyncio
    async def test_returns_data(self) -> None:
        row = _make_row(
            common_name="glucose",
            id="FA:C001",
            entity_type="chemical",
            scientific_name="D-glucose",
            synonyms=[],
            external_ids={},
            chemical_classification=[],
        )
        session = _mock_session_single([row])
        result = await chem_metadata(session, "glucose")
        assert result["metadata"]["row_count"] == 1


class TestChemicalGetComposition:
    @pytest.mark.asyncio
    async def test_splits_by_concentration(self) -> None:
        with_row = _make_row(
            id="FA:F001",
            name="apple",
            median_concentration=5.0,
            fdc_count=2,
            foodatlas_count=1,
            ptfi_count=0,
            evidence_count=3,
        )
        without_row = _make_row(
            id="FA:F002",
            name="banana",
            fdc_count=0,
            foodatlas_count=3,
            ptfi_count=0,
            evidence_count=3,
        )
        session = AsyncMock()
        r_with = MagicMock()
        r_with.__iter__ = lambda self: iter([with_row])
        r_without = MagicMock()
        r_without.__iter__ = lambda self: iter([without_row])
        session.execute.side_effect = [r_with, r_without]

        result = await chem_composition(session, "glucose")
        assert len(result["data"]["with_concentrations"]) == 1
        assert len(result["data"]["without_concentrations"]) == 1
        assert result["metadata"]["row_count"] == 2

    @pytest.mark.asyncio
    async def test_both_buckets_carry_evidence_and_source_counts(self) -> None:
        """The table's Evidence column and source facet read these fields.

        They used to exist only on the unmeasured bucket, so a food with a
        measured concentration had no way to say how many data points backed
        it.
        """
        counts = {
            "fdc_count": 2,
            "foodatlas_count": 1,
            "ptfi_count": 4,
            "evidence_count": 7,
        }
        with_row = _make_row(
            id="FA:F001", name="apple", median_concentration=5.0, **counts
        )
        without_row = _make_row(id="FA:F002", name="banana", **counts)
        session = AsyncMock()
        r_with = MagicMock()
        r_with.__iter__ = lambda self: iter([with_row])
        r_without = MagicMock()
        r_without.__iter__ = lambda self: iter([without_row])
        session.execute.side_effect = [r_with, r_without]

        result = await chem_composition(session, "glucose")
        data = result["data"]
        for bucket in ("with_concentrations", "without_concentrations"):
            row = data[bucket][0]
            assert row["evidence_count"] == 7
            assert row["fdc_count"] == 2
            assert row["foodatlas_count"] == 1
            assert row["ptfi_count"] == 4

    @pytest.mark.asyncio
    async def test_measured_bucket_is_ordered_by_concentration(self) -> None:
        """The bar chart sorted client-side because the SQL had no ORDER BY.

        The table still sorts client-side, but an unordered query made the
        default view non-deterministic between requests.
        """
        session = AsyncMock()
        empty = MagicMock()
        empty.__iter__ = lambda self: iter([])
        session.execute.side_effect = [empty, empty]

        await chem_composition(session, "glucose")
        measured_sql = str(session.execute.call_args_list[0].args[0])
        assert "ORDER BY" in measured_sql
        assert "median_concentration->>'value'" in measured_sql
        assert "NULLS LAST" in measured_sql


class TestChemicalGetCorrelation:
    @pytest.mark.asyncio
    async def test_positive_relation(self) -> None:
        row = _make_row(
            id="FA:D001",
            name="diabetes",
            sources=[],
            evidences=[],
        )
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([row])
        count_result = MagicMock()
        count_result.scalar.return_value = 1
        session.execute.side_effect = [data_result, count_result]

        result = await chem_correlation(session, "glucose", relation="positive")
        assert result["data"]["positive_associations"] is not None
        assert result["data"]["negative_associations"] is None
        assert result["metadata"]["total_rows"] == 1

    @pytest.mark.asyncio
    async def test_negative_relation(self) -> None:
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([])
        count_result = MagicMock()
        count_result.scalar.return_value = 0
        session.execute.side_effect = [data_result, count_result]

        result = await chem_correlation(session, "glucose", relation="negative")
        assert result["data"]["positive_associations"] is None
        assert result["data"]["negative_associations"] is not None
        assert result["metadata"]["total_pages"] == 0


# -- Disease ----------------------------------------------------------------


class TestDiseaseGetMetadata:
    @pytest.mark.asyncio
    async def test_returns_data(self) -> None:
        row = _make_row(
            common_name="diabetes",
            id="FA:D001",
            entity_type="disease",
            scientific_name="diabetes mellitus",
            synonyms=[],
            external_ids={},
        )
        session = _mock_session_single([row])
        result = await disease_metadata(session, "diabetes")
        assert result["metadata"]["row_count"] == 1


class TestDiseaseGetCorrelation:
    @pytest.mark.asyncio
    async def test_positive_relation(self) -> None:
        row = _make_row(
            id="FA:C001",
            name="glucose",
            sources=[],
            evidences=[],
        )
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([row])
        count_result = MagicMock()
        count_result.scalar.return_value = 1
        session.execute.side_effect = [data_result, count_result]

        result = await disease_correlation(session, "diabetes", relation="positive")
        assert result["data"]["positive_associations"] is not None
        assert result["metadata"]["total_rows"] == 1

    @pytest.mark.asyncio
    async def test_negative_relation(self) -> None:
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([])
        count_result = MagicMock()
        count_result.scalar.return_value = 0
        session.execute.side_effect = [data_result, count_result]

        result = await disease_correlation(session, "diabetes", relation="negative")
        assert result["data"]["positive_associations"] is None
        assert result["data"]["negative_associations"] is not None


# -- Search -----------------------------------------------------------------


class TestSearch:
    @pytest.mark.asyncio
    async def test_returns_paginated_results(self) -> None:
        row = _make_row(
            foodatlas_id="FA:0001",
            associations=42,
            entity_type="food",
            common_name="apple",
            scientific_name="Malus domestica",
            synonyms=[],
            external_ids={},
        )
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([row])
        count_result = MagicMock()
        count_result.scalar.return_value = 1
        session.execute.side_effect = [data_result, count_result]

        result = await search(session, "apple")
        assert result["metadata"]["total_rows"] == 1
        assert result["data"][0]["common_name"] == "apple"

    @pytest.mark.asyncio
    async def test_empty_search(self) -> None:
        session = AsyncMock()
        data_result = MagicMock()
        data_result.__iter__ = lambda self: iter([])
        count_result = MagicMock()
        count_result.scalar.return_value = 0
        session.execute.side_effect = [data_result, count_result]

        result = await search(session, "")
        assert result["data"] == []
        assert result["metadata"]["total_pages"] == 0


class TestGetStatistics:
    @pytest.mark.asyncio
    async def test_maps_fields(self) -> None:
        rows = [
            MagicMock(field="number of foods", count=1000),
            MagicMock(field="number of chemicals", count=500),
            MagicMock(field="number of diseases", count=200),
            MagicMock(field="number of publications", count=5000),
            MagicMock(field="number of associations", count=15000),
        ]
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.fetchall.return_value = rows
        session.execute.return_value = result_mock

        result = await get_statistics(session)
        stats = result["data"]["statistics"]
        assert stats["foods"] == 1000
        assert stats["chemicals"] == 500
        assert stats["connections"] == 15000

    @pytest.mark.asyncio
    async def test_unknown_field_skipped(self) -> None:
        rows = [MagicMock(field="unknown field", count=99)]
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.fetchall.return_value = rows
        session.execute.return_value = result_mock

        result = await get_statistics(session)
        assert result["data"]["statistics"] == {}


class TestChemicalCompositionEvidence:
    """One (chemical, food) pair's evidence, fetched when its modal opens."""

    @staticmethod
    def _session(mapping: dict | None) -> AsyncMock:
        session = AsyncMock()
        result = MagicMock()
        result.mappings.return_value.first.return_value = mapping
        session.execute.return_value = result
        return session

    @pytest.mark.asyncio
    async def test_flattens_the_three_source_lists(self) -> None:
        # The modal takes one list and sorts it itself; each record already
        # carries its own source.
        session = self._session(
            {
                "fdc_evidences": [{"premise": "a"}],
                "foodatlas_evidences": [{"premise": "b"}, {"premise": "c"}],
                "ptfi_evidences": None,
            }
        )
        out = await chem_composition_evidence(session, "quercetin", "onion")
        assert out["metadata"] == {"row_count": 3}
        assert [e["premise"] for e in out["data"]] == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_unknown_pair_is_empty_not_an_error(self) -> None:
        out = await chem_composition_evidence(self._session(None), "x", "y")
        assert out == {"data": [], "metadata": {"row_count": 0}}

    @pytest.mark.asyncio
    async def test_both_names_are_bound_not_interpolated(self) -> None:
        session = self._session(None)
        await chem_composition_evidence(session, "quercetin", "yellow onion")
        assert session.execute.call_args[0][1] == {
            "chemical": "quercetin",
            "food": "yellow onion",
        }

    @pytest.mark.asyncio
    async def test_does_not_read_dmd(self) -> None:
        # DMD left the public API in the 2026-07-06 removal; returning it
        # here would show data points the row's count never promised.
        session = self._session(None)
        await chem_composition_evidence(session, "quercetin", "onion")
        assert "dmd" not in str(session.execute.call_args[0][0]).lower()
