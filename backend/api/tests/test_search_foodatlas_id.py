"""Pasting a FoodAtlas ID into search must find the entity.

`mv_search_auto_complete.substr_auto` tokenizes entity_type, common_name,
scientific_name, synonyms and external IDs — but not `foodatlas_id`. So
searching `e2908` matched nothing, even though the results list renders and
highlights the ID as if it were searchable. Both search repositories now OR an
exact/prefix match on the ID column into their WHERE clause.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories._search_util import foodatlas_id_pattern
from src.repositories.search import search
from src.repositories.v1 import search as v1_search


def _session() -> AsyncMock:
    session = AsyncMock()

    def execute(_sql, _params=None):
        result = MagicMock()
        result.scalar.return_value = 0
        result.__iter__.return_value = []
        return result

    session.execute.side_effect = execute
    return session


def _params_at(session: AsyncMock, index: int) -> dict:
    return session.execute.call_args_list[index][0][1]


def _sql_at(session: AsyncMock, index: int) -> str:
    return str(session.execute.call_args_list[index][0][0])


class TestFoodatlasIdPattern:
    def test_id_becomes_a_prefix_pattern(self) -> None:
        assert foodatlas_id_pattern("e2908") == "e2908%"

    def test_partial_id_matches_while_typing(self) -> None:
        assert foodatlas_id_pattern("e29") == "e29%"

    def test_uppercase_is_folded(self) -> None:
        assert foodatlas_id_pattern("E2908") == "e2908%"

    def test_surrounding_whitespace_is_stripped(self) -> None:
        assert foodatlas_id_pattern("  e2908  ") == "e2908%"

    def test_bare_e_is_rejected(self) -> None:
        """Otherwise a single keystroke would match every row."""
        assert foodatlas_id_pattern("e") is None

    def test_ordinary_terms_are_rejected(self) -> None:
        assert foodatlas_id_pattern("tomato") is None
        assert foodatlas_id_pattern("") is None

    def test_id_shaped_prefix_of_a_longer_word_is_rejected(self) -> None:
        """`e2908x` is not an ID, and `elderberry` starts with `e` too."""
        assert foodatlas_id_pattern("e2908x") is None
        assert foodatlas_id_pattern("elderberry") is None

    def test_digits_alone_are_rejected(self) -> None:
        """PubChem CIDs are bare numbers and already match via external_ids."""
        assert foodatlas_id_pattern("2908") is None


class TestSearchMatchesIds:
    @pytest.mark.asyncio
    async def test_id_term_adds_the_clause(self) -> None:
        session = _session()
        await search(session, "e2908")
        assert _params_at(session, 0)["id_pattern"] == "e2908%"
        assert "foodatlas_id LIKE :id_pattern" in _sql_at(session, 0)

    @pytest.mark.asyncio
    async def test_ordinary_term_leaves_the_query_untouched(self) -> None:
        """No stray bind parameter, so the common path keeps its plan."""
        session = _session()
        await search(session, "tomato")
        assert "id_pattern" not in _params_at(session, 0)
        assert "foodatlas_id LIKE" not in _sql_at(session, 0)

    @pytest.mark.asyncio
    async def test_exact_id_outranks_prefix_siblings(self) -> None:
        session = _session()
        await search(session, "e2908")
        assert "WHEN foodatlas_id = :word THEN 0" in _sql_at(session, 0)

    @pytest.mark.asyncio
    async def test_count_query_uses_the_same_clause(self) -> None:
        """A narrower count would report fewer rows than the query returns."""
        session = _session()
        await search(session, "e2908")
        assert _params_at(session, 1)["id_pattern"] == "e2908%"
        assert "foodatlas_id LIKE :id_pattern" in _sql_at(session, 1)


class TestV1SearchMatchesIds:
    @pytest.mark.asyncio
    async def test_id_term_adds_the_clause(self) -> None:
        session = _session()
        await v1_search.search(session, q="e2908")
        assert _params_at(session, 0)["id_pattern"] == "e2908%"

    @pytest.mark.asyncio
    async def test_ordinary_term_leaves_the_query_untouched(self) -> None:
        session = _session()
        await v1_search.search(session, q="tomato")
        assert "id_pattern" not in _params_at(session, 0)

    @pytest.mark.asyncio
    async def test_entity_type_filter_still_narrows_an_id_match(self) -> None:
        """The ID clause is OR'd inside the match group, not AND'd beside it."""
        session = _session()
        await v1_search.search(session, q="e2908", entity_type="chemical")
        sql = _sql_at(session, 0)
        assert "(substr_auto LIKE :pattern OR foodatlas_id LIKE :id_pattern)" in sql
        assert "AND entity_type = :etype" in sql
