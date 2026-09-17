"""The autocomplete search must treat `%` and `_` as literals, not wildcards.

`search()` built its LIKE patterns by hand (``f"%{word}%"``) instead of using
the shared escaping helper, so both metacharacters reached Postgres intact.
Against staging, searching `%` or `_` returned all 9,141 entities — the entire
table. Underscores are not hypothetical here: DMD peptide names look like
`CBL_0001`.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories._search_util import escape_like
from src.repositories.search import search


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


class TestEscapeLike:
    def test_escapes_all_three_metacharacters(self) -> None:
        assert escape_like("%") == r"\%"
        assert escape_like("_") == r"\_"
        assert escape_like("\\") == "\\\\"

    def test_leaves_ordinary_text_alone(self) -> None:
        assert escape_like("tomato") == "tomato"

    def test_escapes_backslash_before_the_others(self) -> None:
        """Order matters: escaping `%` first would then double its backslash."""
        assert escape_like("\\%") == "\\\\\\%"


class TestSearchEscaping:
    @pytest.mark.asyncio
    async def test_percent_is_a_literal(self) -> None:
        session = _session()
        await search(session, "%")
        assert _params_at(session, 0)["pattern"] == r"%\%%"

    @pytest.mark.asyncio
    async def test_underscore_is_a_literal(self) -> None:
        """CBL_0001 must not match CBL10001."""
        session = _session()
        await search(session, "CBL_0001")
        assert _params_at(session, 0)["pattern"] == r"%cbl\_0001%"

    @pytest.mark.asyncio
    async def test_prefix_bucket_is_escaped_too(self) -> None:
        session = _session()
        await search(session, "CBL_0001")
        assert _params_at(session, 0)["prefix"] == r"cbl\_0001%"

    @pytest.mark.asyncio
    async def test_word_stays_raw(self) -> None:
        """`word` feeds array containment and similarity(), not a LIKE."""
        session = _session()
        await search(session, "CBL_0001")
        assert _params_at(session, 0)["word"] == "cbl_0001"

    @pytest.mark.asyncio
    async def test_count_query_uses_the_same_pattern(self) -> None:
        """A raw count pattern would report more rows than the query returns."""
        session = _session()
        await search(session, "%")
        assert _params_at(session, 1)["pattern"] == _params_at(session, 0)["pattern"]

    @pytest.mark.asyncio
    async def test_ordinary_terms_are_unaffected(self) -> None:
        session = _session()
        await search(session, "tomato")
        assert _params_at(session, 0)["pattern"] == "%tomato%"
