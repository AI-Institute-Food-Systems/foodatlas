"""Tests for /v1/ repositories (entities, relationships, triplets, search)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.v1 import entities, relationships, search, triplets
from src.repositories.v1.pagination import (
    DEFAULT_PAGE_SIZE,
    build_page,
    clamp_page_size,
    offset,
)


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


def _first_result(row: MagicMock | None) -> MagicMock:
    result = MagicMock()
    result.first.return_value = row
    return result


def _scalar_result(value: int) -> MagicMock:
    result = MagicMock()
    result.scalar.return_value = value
    return result


# -- Pagination -------------------------------------------------------------


class TestPagination:
    def test_clamp_below_one_returns_default(self) -> None:
        assert clamp_page_size(0) == DEFAULT_PAGE_SIZE
        assert clamp_page_size(-1) == DEFAULT_PAGE_SIZE

    def test_clamp_above_max_returns_max(self) -> None:
        assert clamp_page_size(1000) == 100

    def test_clamp_within_range(self) -> None:
        assert clamp_page_size(25) == 25

    def test_offset_for_first_page_is_zero(self) -> None:
        assert offset(1, 50) == 0

    def test_offset_for_third_page(self) -> None:
        assert offset(3, 50) == 100

    def test_build_page_has_more_true(self) -> None:
        page = build_page(1, 10, 100)
        assert page.has_more

    def test_build_page_has_more_false(self) -> None:
        page = build_page(10, 10, 100)
        assert not page.has_more


# -- Entities ---------------------------------------------------------------


class TestListEntities:
    @pytest.mark.asyncio
    async def test_list_foods_returns_rows_and_total(self) -> None:
        row = _row(
            id="FA:0001",
            common_name="apple",
            scientific_name="Malus",
            synonyms=[],
            external_ids={},
            food_classification=["fruit"],
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await entities.list_entities(session, "food")
        assert total == 1
        assert rows[0]["common_name"] == "apple"

    @pytest.mark.asyncio
    async def test_q_filter_substring(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        _rows, total = await entities.list_entities(session, "chemical", q="glu")
        assert total == 0
        params = session.execute.call_args_list[0][0][1]
        assert params["q"] == "%glu%"

    @pytest.mark.asyncio
    async def test_classification_filter_passed_for_food(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        await entities.list_entities(session, "food", classification="fruit")
        params = session.execute.call_args_list[0][0][1]
        assert params["cls"] == "fruit"

    @pytest.mark.asyncio
    async def test_classification_ignored_for_disease(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        await entities.list_entities(session, "disease", classification="anything")
        params = session.execute.call_args_list[0][0][1]
        assert "cls" not in params


class TestGetEntity:
    @pytest.mark.asyncio
    async def test_lookup_by_id(self) -> None:
        row = _row(
            id="FA:0001",
            common_name="apple",
            scientific_name="",
            synonyms=[],
            external_ids={},
            food_classification=[],
        )
        session = AsyncMock()
        session.execute.return_value = _first_result(row)
        result = await entities.get_entity(session, "food", entity_id="FA:0001")
        assert result is not None
        assert result["common_name"] == "apple"

    @pytest.mark.asyncio
    async def test_lookup_by_common_name(self) -> None:
        row = _row(
            id="FA:C001",
            common_name="glucose",
            scientific_name="",
            synonyms=[],
            external_ids={},
            chemical_classification=[],
            flavor_descriptors=[],
        )
        session = AsyncMock()
        session.execute.return_value = _first_result(row)
        result = await entities.get_entity(session, "chemical", common_name="glucose")
        assert result is not None

    @pytest.mark.asyncio
    async def test_returns_none_when_missing(self) -> None:
        session = AsyncMock()
        session.execute.return_value = _first_result(None)
        result = await entities.get_entity(session, "food", entity_id="FA:NOPE")
        assert result is None

    @pytest.mark.asyncio
    async def test_no_lookup_args_returns_none(self) -> None:
        session = AsyncMock()
        result = await entities.get_entity(session, "food")
        assert result is None
        session.execute.assert_not_called()


class TestResolveId:
    @pytest.mark.asyncio
    async def test_returns_tuple_when_found(self) -> None:
        row = MagicMock()
        row.__getitem__ = lambda self, i: ("food", "apple")[i]
        session = AsyncMock()
        session.execute.return_value = _first_result(row)
        result = await entities.resolve_id(session, "FA:0001")
        assert result == ("food", "apple")

    @pytest.mark.asyncio
    async def test_returns_none_when_missing(self) -> None:
        session = AsyncMock()
        session.execute.return_value = _first_result(None)
        assert await entities.resolve_id(session, "FA:NOPE") is None


# -- Relationships ----------------------------------------------------------


class TestListComposition:
    @pytest.mark.asyncio
    async def test_filter_by_food_id(self) -> None:
        row = _row(
            food_id="FA:0001",
            food_name="apple",
            chemical_id="FA:C001",
            chemical_name="glucose",
            chemical_classification=["carbohydrate"],
            median_concentration={"value": 5.0, "unit": "mg/100g"},
            attestation_count=3,
            sources=["fdc"],
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await relationships.list_composition(session, food_id="FA:0001")
        assert total == 1
        assert rows[0]["chemical_name"] == "glucose"

    @pytest.mark.asyncio
    async def test_filter_by_chemical_id(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        _rows, total = await relationships.list_composition(
            session, chemical_id="FA:C001"
        )
        assert total == 0

    @pytest.mark.asyncio
    async def test_no_filter_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await relationships.list_composition(session)
        assert rows == [] and total == 0
        session.execute.assert_not_called()


class TestListBioactivityChemicals:
    @pytest.mark.asyncio
    async def test_filter_by_bioactivity_id(self) -> None:
        row = _row(
            bioactivity_id="FA:B001",
            bioactivity_name="antioxidant",
            chemical_id="FA:C001",
            chemical_name="quercetin",
            measurement_count=755,
            active_count=83,
            inactive_count=261,
            measurements=[
                {"endpoint": "IC50", "value": 17.175, "unit": "MICROMOLAR"},
                {"endpoint": "IC50", "value": 5.0, "unit": "MICROMOLAR"},
            ],
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await relationships.list_bioactivity_chemicals(
            session, bioactivity_id="FA:B001"
        )
        assert total == 1
        assert rows[0]["top_measurement"]["value"] == 17.175
        assert rows[0]["top_measurement"]["endpoint"] == "IC50"
        # ``measurements`` is popped in favour of ``top_measurement``.
        assert "measurements" not in rows[0]

    @pytest.mark.asyncio
    async def test_filter_by_chemical_id(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        _rows, total = await relationships.list_bioactivity_chemicals(
            session, chemical_id="FA:C001"
        )
        assert total == 0

    @pytest.mark.asyncio
    async def test_no_filter_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await relationships.list_bioactivity_chemicals(session)
        assert rows == [] and total == 0
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_measurements_yields_null_top(self) -> None:
        row = _row(
            bioactivity_id="FA:B001",
            bioactivity_name="antioxidant",
            chemical_id="FA:C001",
            chemical_name="quercetin",
            measurement_count=0,
            active_count=0,
            inactive_count=0,
            measurements=[],
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, _total = await relationships.list_bioactivity_chemicals(
            session, bioactivity_id="FA:B001"
        )
        assert rows[0]["top_measurement"] is None


class TestListBioactivityFoods:
    @pytest.mark.asyncio
    async def test_filter_by_bioactivity_id(self) -> None:
        row = _row(
            bioactivity_id="FA:B001",
            bioactivity_name="antioxidant",
            food_id="FA:0001",
            food_name="snail",
            measurement_count=1,
            measurements=[
                {"endpoint": "Activity", "value": 0.519, "unit": "mmol/100g"}
            ],
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await relationships.list_bioactivity_foods(
            session, bioactivity_id="FA:B001"
        )
        assert total == 1
        assert rows[0]["top_measurement"]["value"] == 0.519

    @pytest.mark.asyncio
    async def test_filter_by_food_id(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        _rows, total = await relationships.list_bioactivity_foods(
            session, food_id="FA:0001"
        )
        assert total == 0

    @pytest.mark.asyncio
    async def test_no_filter_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await relationships.list_bioactivity_foods(session)
        assert rows == [] and total == 0
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_measurement_missing_value_is_skipped(self) -> None:
        row = _row(
            bioactivity_id="FA:B001",
            bioactivity_name="antioxidant",
            food_id="FA:0001",
            food_name="snail",
            measurement_count=2,
            measurements=[
                {"endpoint": "Activity", "value": None, "unit": "mmol/100g"},
                {"endpoint": "Activity", "value": 0.3, "unit": "mmol/100g"},
            ],
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, _total = await relationships.list_bioactivity_foods(
            session, bioactivity_id="FA:B001"
        )
        assert rows[0]["top_measurement"]["value"] == 0.3


class TestListCorrelation:
    @pytest.mark.asyncio
    async def test_filter_by_chemical_with_relation(self) -> None:
        row = _row(
            chemical_id="FA:C001",
            chemical_name="glucose",
            disease_id="FA:D001",
            disease_name="diabetes",
            source_chemical_id="FA:C001",
            source_chemical_name="glucose",
            sources=["pubmed"],
            evidence_count=2,
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await relationships.list_correlation(
            session, chemical_id="FA:C001", relation="reduces"
        )
        assert total == 1
        assert rows[0]["relation"] == "reduces"

    @pytest.mark.asyncio
    async def test_unknown_relation_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await relationships.list_correlation(
            session, chemical_id="FA:C001", relation="bogus"
        )
        assert rows == [] and total == 0
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_filter_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await relationships.list_correlation(session)
        assert rows == [] and total == 0


# -- Triplets / Attestations ------------------------------------------------


class TestListTriplets:
    @pytest.mark.asyncio
    async def test_filters_and_attestation_lookup(self) -> None:
        triplet_row = _row(
            triplet_id=1,
            head_id="FA:0001",
            head_name="apple",
            relationship_id="r1",
            relationship_name="contains",
            tail_id="FA:C001",
            tail_name="glucose",
            source="fdc",
            attestation_ids=["att1"],
        )
        att_row = _row(
            attestation_id="att1",
            source="fdc",
            evidence_id="ev1",
        )
        session = AsyncMock()
        session.execute.side_effect = [
            _scalar_result(1),  # count
            _iter_result([triplet_row]),  # list
            _iter_result([att_row]),  # summary lookup
            _iter_result([]),  # trust scores
        ]
        rows, total = await triplets.list_triplets(
            session, head_id="FA:0001", relationship="contains"
        )
        assert total == 1
        assert rows[0]["attestations"][0]["attestation_id"] == "att1"

    @pytest.mark.asyncio
    async def test_invalid_relationship_alias_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await triplets.list_triplets(session, relationship="bogus")
        assert rows == [] and total == 0
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_relationship_raw_id_accepted(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        await triplets.list_triplets(session, relationship="r1")
        params = session.execute.call_args_list[0][0][1]
        assert params["rel_id"] == "r1"


class TestGetTriplet:
    @pytest.mark.asyncio
    async def test_returns_triplet_with_attestations(self) -> None:
        triplet_row = _row(
            triplet_id=1,
            head_id="FA:0001",
            head_name="apple",
            relationship_id="r1",
            relationship_name="contains",
            tail_id="FA:C001",
            tail_name="glucose",
            source="fdc",
            attestation_ids=["att1"],
        )
        att_row = _row(attestation_id="att1", source="fdc", evidence_id="ev1")
        session = AsyncMock()
        session.execute.side_effect = [
            _first_result(triplet_row),
            _iter_result([att_row]),
            _iter_result([]),
        ]
        result = await triplets.get_triplet(session, 1)
        assert result is not None
        assert result["triplet_id"] == 1
        assert len(result["attestations"]) == 1

    @pytest.mark.asyncio
    async def test_returns_none_when_missing(self) -> None:
        session = AsyncMock()
        session.execute.return_value = _first_result(None)
        assert await triplets.get_triplet(session, 99) is None


class TestGetAttestation:
    @pytest.mark.asyncio
    async def test_returns_full_row(self) -> None:
        att_row = _row(
            attestation_id="att1",
            source="fdc",
            evidence_id="ev1",
            head_name_raw="apple",
            tail_name_raw="glucose",
            conc_value=5.0,
            conc_unit="mg/100g",
            food_part="fruit",
            food_processing="raw",
            validated=False,
            validated_correct=True,
        )
        triplet_join_row = MagicMock()
        triplet_join_row.__getitem__ = lambda self, i: ("FA:0001", "FA:C001", "r1")[i]
        summary_row = _row(attestation_id="att1", source="fdc", evidence_id="ev1")
        session = AsyncMock()
        session.execute.side_effect = [
            _first_result(att_row),
            _first_result(triplet_join_row),
            _iter_result([summary_row]),
            _iter_result([]),
            _first_result(None),  # trust reason
        ]
        result = await triplets.get_attestation(session, "att1")
        assert result is not None
        assert result["head_id"] == "FA:0001"
        assert result["tail_id"] == "FA:C001"
        assert result["trust_score"] is None

    @pytest.mark.asyncio
    async def test_returns_none_when_missing(self) -> None:
        session = AsyncMock()
        session.execute.return_value = _first_result(None)
        assert await triplets.get_attestation(session, "missing") is None


# -- Search + stats ---------------------------------------------------------


class TestSearchRepo:
    @pytest.mark.asyncio
    async def test_returns_hits(self) -> None:
        row = _row(
            id="FA:0001",
            entity_type="food",
            common_name="apple",
            scientific_name="Malus",
            associations=42,
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(1), _iter_result([row])]
        rows, total = await search.search(session, q="apple")
        assert total == 1
        assert rows[0]["common_name"] == "apple"

    @pytest.mark.asyncio
    async def test_empty_query_returns_empty(self) -> None:
        session = AsyncMock()
        rows, total = await search.search(session, q="   ")
        assert rows == [] and total == 0
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_entity_type_filter_applied(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        await search.search(session, q="x", entity_type="chemical")
        params = session.execute.call_args_list[0][0][1]
        assert params["etype"] == "chemical"

    @pytest.mark.asyncio
    async def test_like_metacharacters_are_literals(self) -> None:
        """`%` used to match every entity on the public endpoint."""
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(0), _iter_result([])]
        await search.search(session, q="CBL_0001")
        params = session.execute.call_args_list[0][0][1]
        assert params["pattern"] == r"%cbl\_0001%"
        assert params["prefix"] == r"cbl\_0001%"
        # `word` feeds array containment and similarity(), not a LIKE.
        assert params["word"] == "cbl_0001"


def _stat_row(field_value: str, count_value: int) -> MagicMock:
    row = MagicMock()
    row._mapping = {"field": field_value, "count": count_value}
    return row


class TestStatsRepo:
    @pytest.mark.asyncio
    async def test_maps_known_fields(self) -> None:
        rows = [
            _stat_row("number of foods", 1),
            _stat_row("number of chemicals", 2),
            _stat_row("number of diseases", 3),
            _stat_row("number of publications", 4),
            _stat_row("number of associations", 5),
            _stat_row("number of bioactivities", 6),
            _stat_row("number of bioactivity measurements", 7),
        ]
        session = AsyncMock()
        session.execute.return_value = _iter_result(rows)
        out = await search.get_stats(session)
        assert out == {
            "foods": 1,
            "chemicals": 2,
            "diseases": 3,
            "publications": 4,
            "connections": 5,
            "bioactivities": 6,
            "bioactivity_measurements": 7,
        }

    @pytest.mark.asyncio
    async def test_unknown_field_ignored(self) -> None:
        rows = [_stat_row("other", 99)]
        session = AsyncMock()
        session.execute.return_value = _iter_result(rows)
        out = await search.get_stats(session)
        assert out["foods"] == 0
