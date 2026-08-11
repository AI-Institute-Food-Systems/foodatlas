"""Flagged-concentration filter on /food/inferred-bioactivities.

The warning icon for implausible concentrations shipped working but
unreachable: apple carries 9 flagged rows among 1,591 across 80 pages, so it
only ever appeared to someone already on the right page. These tests pin the
filter and, more importantly, the faceting rule behind ``n_flagged``.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories.bioactivity import get_food_inferred_bioactivities


def _session() -> AsyncMock:
    """Session whose COUNT queries return 7, and whose row query returns none."""
    session = AsyncMock()

    def execute(_sql, _params=None):
        result = MagicMock()
        result.scalar.return_value = 7
        result.__iter__.return_value = []
        return result

    session.execute.side_effect = execute
    return session


def _sql_at(session: AsyncMock, index: int) -> str:
    return str(session.execute.call_args_list[index][0][0])


class TestConcFlagFilter:
    @pytest.mark.asyncio
    async def test_flag_clause_absent_when_unset(self):
        session = _session()
        await get_food_inferred_bioactivities(session, "apple")
        # First call is the total count; it must not be narrowed to flagged.
        assert "conc_quality_flag" not in _sql_at(session, 0)

    @pytest.mark.asyncio
    async def test_flag_clause_applied_when_set(self):
        session = _session()
        await get_food_inferred_bioactivities(
            session, "apple", filter_conc_flag="suspect_high"
        )
        assert "eff.conc_quality_flag = 'suspect_high'" in _sql_at(session, 0)

    @pytest.mark.asyncio
    async def test_unknown_flag_value_is_ignored(self):
        """Only the one known flag narrows; anything else is a no-op."""
        session = _session()
        await get_food_inferred_bioactivities(
            session, "apple", filter_conc_flag="'; DROP TABLE--"
        )
        assert "conc_quality_flag" not in _sql_at(session, 0)

    @pytest.mark.asyncio
    async def test_count_query_joins_efficacy_view(self):
        """Without the join the flag column isn't in scope and COUNT errors."""
        session = _session()
        await get_food_inferred_bioactivities(
            session, "apple", filter_conc_flag="suspect_high"
        )
        assert "mv_food_chemical_efficacy eff" in _sql_at(session, 0)

    @pytest.mark.asyncio
    async def test_n_flagged_is_reported(self):
        session = _session()
        out = await get_food_inferred_bioactivities(session, "apple")
        assert out["metadata"]["n_flagged"] == 7

    @pytest.mark.asyncio
    async def test_n_flagged_keeps_other_filters_but_drops_its_own(self):
        """Faceting: the flagged count answers "what would clicking give me?".

        It must carry the search filter through, yet not be narrowed by the
        flag filter itself — otherwise the chip would just echo the current
        row count once active.
        """
        session = _session()
        await get_food_inferred_bioactivities(
            session, "apple", search="quercetin", filter_conc_flag="suspect_high"
        )
        flagged_sql = _sql_at(session, 1)
        assert "ILIKE :q" in flagged_sql
        # Exactly one flag predicate — not the filter's plus the facet's.
        assert flagged_sql.count("conc_quality_flag = 'suspect_high'") == 1
