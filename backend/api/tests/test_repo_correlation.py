"""The query-shaping added for the merged Diseases/Chemicals tab.

Two behaviours worth pinning, because both fail silently rather than
loudly if they regress:

* ``relation="all"`` must drop the direction predicate. If it instead
  fell through to one of the named directions, the merged tab would
  quietly show half its rows and the badge would still look plausible.
* ``search`` must reach the query escaped. ``%`` and ``_`` are ILIKE
  metacharacters, so an unescaped ``50%`` matches every row containing
  "50" — a search that appears to work while returning the wrong set.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories import _correlation
from src.repositories.chemical import (
    get_correlation as chem_correlation,
)
from src.repositories.chemical import (
    get_correlation_direction_counts as chem_direction_counts,
)
from src.repositories.disease import (
    get_correlation as disease_correlation,
)


def _session(rows: list[object], total: int) -> AsyncMock:
    session = AsyncMock()
    data_result = MagicMock()
    data_result.__iter__ = lambda self: iter(rows)
    count_result = MagicMock()
    count_result.scalar.return_value = total
    session.execute.side_effect = [data_result, count_result]
    return session


def _sql(session: AsyncMock, index: int = 0) -> str:
    return str(session.execute.call_args_list[index].args[0])


def _params(session: AsyncMock, index: int = 0) -> dict:
    return session.execute.call_args_list[index].args[1]


class TestBuildFilters:
    def test_all_drops_the_direction_predicate(self) -> None:
        where, params = _correlation.build_filters("all", "", "disease_name")
        assert "relationship_id" not in where
        assert "rel" not in params

    def test_unknown_relation_is_treated_as_all(self) -> None:
        # A typo in a query string must not silently mean "positive".
        where, params = _correlation.build_filters("improves", "", "disease_name")
        assert "relationship_id" not in where
        assert params == {}

    @pytest.mark.parametrize(
        ("relation", "expected"),
        [("positive", "r4"), ("negative", "r3")],
    )
    def test_named_directions_bind_their_relationship_id(
        self, relation: str, expected: str
    ) -> None:
        where, params = _correlation.build_filters(relation, "", "disease_name")
        assert "relationship_id = :rel" in where
        assert params["rel"] == expected

    def test_blank_search_adds_no_clause(self) -> None:
        # Whitespace-only input is truthy in Python; an unguarded pattern
        # would become ILIKE '% %' and match every multi-word name.
        for term in ("", "   ", "\t"):
            where, params = _correlation.build_filters("all", term, "disease_name")
            assert "ILIKE" not in where
            assert "pattern" not in params

    def test_like_metacharacters_are_escaped(self) -> None:
        _, params = _correlation.build_filters("all", "50%", "disease_name")
        assert params["pattern"] == r"%50\%%"
        _, params = _correlation.build_filters("all", "5_x", "disease_name")
        assert params["pattern"] == r"%5\_x%"

    def test_peer_column_is_qualified_by_the_caller(self) -> None:
        # The disease page query aliases the view as `c`; its count query
        # does not. Passing the wrong one is an ambiguous-column error.
        where, _ = _correlation.build_filters("all", "cancer", "c.chemical_name")
        assert "c.chemical_name ILIKE :pattern" in where


class TestChemicalCorrelation:
    @pytest.mark.asyncio
    async def test_all_returns_both_directions_under_one_key(self) -> None:
        session = _session([], 0)
        result = await chem_correlation(session, "caffeine", relation="all")
        assert result["data"]["associations"] == []
        # Legacy per-direction keys stay None so an older caller can tell
        # this page was not filtered to its direction.
        assert result["data"]["positive_associations"] is None
        assert result["data"]["negative_associations"] is None
        assert "relationship_id = :rel" not in _sql(session)

    @pytest.mark.asyncio
    async def test_selects_relationship_id_so_rows_carry_direction(self) -> None:
        session = _session([], 0)
        await chem_correlation(session, "caffeine", relation="all")
        assert "relationship_id" in _sql(session)

    @pytest.mark.asyncio
    async def test_named_direction_still_populates_its_legacy_key(self) -> None:
        session = _session([], 0)
        result = await chem_correlation(session, "caffeine", relation="positive")
        assert result["data"]["positive_associations"] is not None
        assert result["data"]["negative_associations"] is None

    @pytest.mark.asyncio
    async def test_search_reaches_both_the_page_and_count_queries(self) -> None:
        # A search applied to the page but not the count paginates against
        # the unfiltered total: page 2 of a 1-page result set.
        session = _session([], 0)
        await chem_correlation(session, "caffeine", search="neo")
        assert "ILIKE :pattern" in _sql(session, 0)
        assert "ILIKE :pattern" in _sql(session, 1)
        assert _params(session, 0)["pattern"] == "%neo%"
        assert _params(session, 1)["pattern"] == "%neo%"

    @pytest.mark.asyncio
    async def test_ordering_is_deterministic(self) -> None:
        # evidence_count alone ties constantly, and an unstable ORDER BY
        # under OFFSET/FETCH drops and repeats rows across pages.
        session = _session([], 0)
        await chem_correlation(session, "caffeine")
        assert "ORDER BY SUM(evidence_count) DESC, disease_name" in _sql(session)


class TestDiseaseCorrelation:
    @pytest.mark.asyncio
    async def test_all_returns_both_directions(self) -> None:
        session = _session([], 0)
        result = await disease_correlation(session, "diabetes", relation="all")
        assert result["data"]["associations"] == []
        assert "relationship_id = :rel" not in _sql(session)

    @pytest.mark.asyncio
    async def test_search_qualifies_the_aliased_and_bare_columns(self) -> None:
        session = _session([], 0)
        await disease_correlation(session, "diabetes", search="acid")
        page_sql = _sql(session, 0)
        # The CTE and outer query use different qualifications of the same
        # column; both must carry the filter or they disagree on the set.
        assert "c.chemical_name ILIKE :pattern" in page_sql
        assert "chemical_name ILIKE :pattern" in page_sql
        assert "chemical_name ILIKE :pattern" in _sql(session, 1)


class TestPairGrouping:
    """One row per (anchor, source_chemical, peer), not per direction.

    ~4% of pairs are reported both ways. The view stores those as two
    rows; the UI shows one "Mixed" row. Grouping must happen in SQL —
    the two halves are ordered by evidence_count and routinely land on
    different pages, so a per-page frontend merge would show the pair
    twice under two different glyphs.
    """

    @pytest.mark.asyncio
    async def test_page_query_groups_by_pair(self) -> None:
        session = _session([], 0)
        await chem_correlation(session, "caffeine")
        sql = _sql(session, 0)
        assert f"GROUP BY {_correlation.GROUP_BY_PAIR}" in sql
        assert "relationship_ids" in sql

    @pytest.mark.asyncio
    async def test_count_query_counts_pairs_not_rows(self) -> None:
        # Counting the ungrouped rows would overstate total_pages and
        # leave trailing pages empty for any anchor with mixed pairs.
        session = _session([], 0)
        await chem_correlation(session, "caffeine")
        count_sql = _sql(session, 1)
        assert f"GROUP BY {_correlation.GROUP_BY_PAIR}" in count_sql

    @pytest.mark.asyncio
    async def test_orders_by_the_summed_evidence_count(self) -> None:
        session = _session([], 0)
        await chem_correlation(session, "caffeine")
        assert "ORDER BY SUM(evidence_count) DESC" in _sql(session, 0)

    @pytest.mark.asyncio
    async def test_disease_side_groups_too(self) -> None:
        session = _session([], 0)
        await disease_correlation(session, "diabetes")
        assert f"GROUP BY {_correlation.GROUP_BY_PAIR}" in _sql(session, 0)
        # jsonb has no MIN, so ambiguity_siblings must come from a scalar
        # subquery on the grouped column rather than a join + aggregate.
        assert "MIN(" not in _sql(session, 0)


class TestMergeEvidences:
    def test_unions_both_directions(self) -> None:
        merged = _correlation.merge_evidences(
            [{"pmid": {"id": "1"}}], [{"pmid": {"id": "2"}}]
        )
        assert [e["pmid"]["id"] for e in merged] == ["1", "2"]

    def test_dedupes_a_paper_cited_for_both_directions(self) -> None:
        # One study reporting a benefit in one context and a harm in
        # another is cited under both. Counting it twice would make the
        # row's "See N publications" button overstate exactly the mixed
        # pairs this grouping exists to surface.
        merged = _correlation.merge_evidences(
            [{"pmid": {"id": "7"}}, {"pmid": {"id": "8"}}],
            [{"pmid": {"id": "7"}}],
        )
        assert [e["pmid"]["id"] for e in merged] == ["7", "8"]

    def test_falls_back_to_pmcid_as_the_identity(self) -> None:
        merged = _correlation.merge_evidences(
            [{"pmcid": {"id": "PMC1"}}], [{"pmcid": {"id": "PMC1"}}]
        )
        assert len(merged) == 1

    def test_keeps_unidentifiable_rows_rather_than_collapsing_them(self) -> None:
        # Two evidence records with neither id are not known duplicates;
        # dropping one would silently lose a citation.
        merged = _correlation.merge_evidences([{}, {}], None)
        assert len(merged) == 2

    @pytest.mark.parametrize(
        ("improves", "worsens"),
        [(None, None), ([], None), (None, [])],
    )
    def test_handles_the_missing_direction(
        self, improves: list | None, worsens: list | None
    ) -> None:
        # A single-direction pair has NULL for the other side, straight
        # from the FILTER-ed aggregate.
        assert _correlation.merge_evidences(improves, worsens) == []

    def test_shape_pair_rows_attaches_the_union(self) -> None:
        rows = [
            {
                "improves_evidences": [{"pmid": {"id": "1"}}],
                "worsens_evidences": [{"pmid": {"id": "2"}}],
            }
        ]
        assert len(_correlation.shape_pair_rows(rows)[0]["evidences"]) == 2


class TestDirectionCountsAfterGrouping:
    @pytest.mark.asyncio
    async def test_counts_are_not_filtered_by_direction(self) -> None:
        # A facet that only counted the selected direction would read zero
        # for the option the user is trying to switch to.
        session = AsyncMock()
        result = MagicMock()
        result.one.return_value = MagicMock(improves=0, worsens=0, both=0)
        session.execute.return_value = result

        await chem_direction_counts(session, "caffeine")
        assert "relationship_id = :rel" not in _sql(session)

    @pytest.mark.asyncio
    async def test_missing_direction_counts_zero_not_absent(self) -> None:
        # A chemical with only worsening evidence must still report
        # improves=0, or the facet renders a blank instead of a zero.
        session = AsyncMock()
        result = MagicMock()
        result.one.return_value = MagicMock(improves=0, worsens=152, both=152)
        session.execute.return_value = result

        counts = await chem_direction_counts(session, "caffeine")
        assert counts == {"improves": 0, "worsens": 152, "both": 152}

    @pytest.mark.asyncio
    async def test_counts_pairs_so_all_is_not_the_sum(self) -> None:
        # improves + worsens double-counts mixed pairs; "both" is the
        # number of rows the table will actually render.
        session = AsyncMock()
        result = MagicMock()
        result.one.return_value = MagicMock(improves=52, worsens=152, both=187)
        session.execute.return_value = result

        counts = await chem_direction_counts(session, "caffeine")
        assert counts == {"improves": 52, "worsens": 152, "both": 187}
        assert counts["both"] < counts["improves"] + counts["worsens"]
        assert "bool_or" in _sql(session)
