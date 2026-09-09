"""The composition source filter, tested by execution rather than by mock.

Every other API test hands the repository an ``AsyncMock`` session, so
nothing has ever looked at the SQL that comes out. That is exactly how
``_build_query_parts`` shipped a source predicate that only fired when
one source was selected: ``test_multiple_sources`` passed
``filter_source="fdc+dmd"`` to an empty mock and asserted
``total_pages == 0``, which is true whether the WHERE clause is right,
wrong, or absent.

So the tests here run the generated WHERE against a real (SQLite)
table, and drive the counts path over the same fixture, and assert the
two agree. Nothing asserts on a substring of SQL where it can assert on
the rows the SQL returns.
"""

from __future__ import annotations

import itertools
import sqlite3
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest
from src.repositories._sources import (
    COMPOSITION_SOURCES,
    parse_sources,
    row_has_source,
    source_sql,
)
from src.repositories.food import (
    EVIDENCE_COUNT_SORT_SOURCES,
    VALID_SORT_COLS,
    _build_query_parts,
    _collect_attestation_ids,
    _resort_after_filter,
    get_composition_counts,
)
from src.repositories.v1.relationships import _COMPOSITION_SELECT_CLAUSE

if TYPE_CHECKING:
    from collections.abc import Collection, Iterator

TARGET_FOOD = "pepper (raw)"
OTHER_FOOD = "onion"


def _fixture_rows() -> list[dict]:
    """One row per non-empty subset of the three sources, plus a decoy.

    Eight rows: seven combinations under ``pepper (raw)`` and an
    all-sources row under a different food, so a predicate that loses its
    parentheses (and therefore ORs across ``food_name = :name``) pulls in
    a row it should never see.
    """
    rows: list[dict] = []
    for i, present in enumerate(_non_empty_subsets(COMPOSITION_SOURCES)):
        rows.append(_row(f"c{i}", TARGET_FOOD, present))
    rows.append(_row("decoy", OTHER_FOOD, COMPOSITION_SOURCES))
    return rows


def _row(chem: str, food: str, present: Collection[str]) -> dict:
    row: dict = {"chemical_name": chem, "food_name": food}
    for s in COMPOSITION_SOURCES:
        # One attestation per present source; None (not []) when absent,
        # which is what the materialized view stores and what every
        # `IS NOT NULL` in the query is testing for.
        row[f"{s}_evidences"] = (
            [{"extraction": [{"attestation_id": f"{chem}-{s}"}]}]
            if s in present
            else None
        )
    return row


def _non_empty_subsets(items: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    for n in range(1, len(items) + 1):
        yield from itertools.combinations(items, n)


def _all_subsets(items: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    for n in range(len(items) + 1):
        yield from itertools.combinations(items, n)


def _sqlite_fixture() -> sqlite3.Connection:
    """The fixture as a queryable table.

    SQLite is close enough for what is under test — ``IS NOT NULL``,
    ``AND``/``OR`` precedence and named parameters all behave as
    Postgres does. It has no ILIKE or ``= ANY(...)``, so only the source
    and food_name conditions are executed here; search and classification
    stay on string assertions elsewhere.
    """
    conn = sqlite3.connect(":memory:")
    cols = ", ".join(f"{s}_evidences TEXT" for s in COMPOSITION_SOURCES)
    conn.execute(
        "CREATE TABLE mv_food_chemical_composition "
        f"(chemical_name TEXT, food_name TEXT, {cols})"
    )
    placeholders = ", ".join(["?"] * (2 + len(COMPOSITION_SOURCES)))
    conn.executemany(
        f"INSERT INTO mv_food_chemical_composition VALUES ({placeholders})",
        [
            (
                r["chemical_name"],
                r["food_name"],
                *(
                    None if r[f"{s}_evidences"] is None else "x"
                    for s in COMPOSITION_SOURCES
                ),
            )
            for r in _fixture_rows()
        ],
    )
    return conn


def _executable_where(sources: list[str]) -> str:
    """The real WHERE from _build_query_parts, minus the Postgres-only bits.

    We take the conditions the production code actually generated — not a
    re-derivation of them — and drop only those SQLite cannot parse. With
    no search term and no classification filter, that drops nothing.
    """
    _, where_parts, _ = _build_query_parts(TARGET_FOOD, sources, "", True, None)
    # Joined exactly as get_composition joins them.
    return " AND ".join(where_parts)


class TestGeneratedWhereClause:
    """Execute the generated SQL; compare to the Python twin."""

    @pytest.mark.parametrize("subset", list(_all_subsets(COMPOSITION_SOURCES)))
    def test_matches_row_has_source_oracle(self, subset: tuple[str, ...]) -> None:
        conn = _sqlite_fixture()
        where = _executable_where(list(subset))
        got = {
            r[0]
            for r in conn.execute(
                "SELECT chemical_name FROM mv_food_chemical_composition "
                "WHERE " + where.replace(":name", "?"),
                (TARGET_FOOD,),
            )
        }
        # The empty subset reaches _build_query_parts only via
        # filter_source="" (no filter), which means "every source".
        effective = subset or COMPOSITION_SOURCES
        want = {
            r["chemical_name"]
            for r in _fixture_rows()
            if r["food_name"] == TARGET_FOOD and row_has_source(r, effective)
        }
        assert got == want, f"subset={subset}"

    def test_fdc_plus_ptfi_excludes_foodatlas_only_rows(self) -> None:
        """The reported bug, at fixture scale.

        Before the fix this returned all seven target rows because no
        source predicate was emitted for a two-of-three selection.
        """
        conn = _sqlite_fixture()
        where = _executable_where(["fdc", "ptfi"])
        got = {
            r[0]
            for r in conn.execute(
                "SELECT chemical_name FROM mv_food_chemical_composition "
                "WHERE " + where.replace(":name", "?"),
                (TARGET_FOOD,),
            )
        }
        # Exactly one fixture row has foodatlas evidence and nothing else.
        excluded = [
            r["chemical_name"]
            for r in _fixture_rows()
            if r["food_name"] == TARGET_FOOD and not row_has_source(r, ("fdc", "ptfi"))
        ]
        assert len(excluded) == 1
        assert excluded[0] not in got
        assert len(got) == 6

    def test_predicate_does_not_leak_across_foods(self) -> None:
        """A bare (unparenthesised) OR-group would return the decoy row."""
        conn = _sqlite_fixture()
        for subset in _non_empty_subsets(COMPOSITION_SOURCES):
            where = _executable_where(list(subset))
            names = {
                r[0]
                for r in conn.execute(
                    "SELECT chemical_name FROM mv_food_chemical_composition "
                    "WHERE " + where.replace(":name", "?"),
                    (TARGET_FOOD,),
                )
            }
            assert "decoy" not in names, f"subset={subset} leaked another food's rows"

    def test_source_sql_is_parenthesised(self) -> None:
        clause = source_sql(["fdc", "ptfi"])
        assert clause.startswith("(")
        assert clause.endswith(")")

    def test_every_source_column_is_selected(self) -> None:
        select_cols, _, _ = _build_query_parts(TARGET_FOOD, ["ptfi"], "", True, None)
        # The filter picks rows, not columns — the modal greys out
        # deselected sources and needs their evidence to do it.
        for s in COMPOSITION_SOURCES:
            assert f"{s}_evidences" in select_cols


def _make_row(**kwargs: object) -> MagicMock:
    row = MagicMock()
    row._mapping = kwargs
    return row


def _mock_session_sequence(*result_rows: list[MagicMock]) -> AsyncMock:
    """Session where each execute() returns the next batch of rows."""
    session = AsyncMock()
    results = []
    for rows in result_rows:
        r = MagicMock()
        r.__iter__ = lambda self, _rows=rows: iter(_rows)
        results.append(r)
    session.execute.side_effect = results
    return session


def _counts_session() -> AsyncMock:
    """The fixture's target-food rows, shaped as get_composition_counts reads them."""
    rows = [
        _make_row(
            id=i,
            chemical_name=r["chemical_name"],
            chemical_classification=["carbohydrate"],
            median_concentration={"value": 1.0},
            **{f"{s}_evidences": r[f"{s}_evidences"] for s in COMPOSITION_SOURCES},
        )
        for i, r in enumerate(_fixture_rows())
        if r["food_name"] == TARGET_FOOD
    ]
    # Second execute() is _fetch_trust_scores — no scores, nothing low-trust.
    return _mock_session_sequence(rows, [])


class TestCountsAgreeWithRows:
    """The drift guard: counts path and rows path, same fixture, same answers."""

    @pytest.mark.asyncio
    async def test_source_counts_match_the_python_predicate(self) -> None:
        result = await get_composition_counts(_counts_session(), TARGET_FOOD)
        counts = result["data"]["source_counts"]
        for s in COMPOSITION_SOURCES:
            want = sum(
                1
                for r in _fixture_rows()
                if r["food_name"] == TARGET_FOOD and row_has_source(r, (s,))
            )
            assert counts[s] == want, s

    @pytest.mark.asyncio
    @pytest.mark.parametrize("subset", list(_non_empty_subsets(COMPOSITION_SOURCES)))
    async def test_counts_select_the_same_rows_as_the_sql(
        self, subset: tuple[str, ...]
    ) -> None:
        """total for a subset, via counts == row set the WHERE returns."""
        conn = _sqlite_fixture()
        where = _executable_where(list(subset))
        sql_names = {
            r[0]
            for r in conn.execute(
                "SELECT chemical_name FROM mv_food_chemical_composition "
                "WHERE " + where.replace(":name", "?"),
                (TARGET_FOOD,),
            )
        }
        # no_concentration_count applies the source filter, so drive the
        # counts path with concentrations stripped: the count it reports is
        # then exactly "rows this source filter selects".
        rows = [
            _make_row(
                id=i,
                chemical_name=r["chemical_name"],
                chemical_classification=[],
                median_concentration=None,
                **{f"{s}_evidences": r[f"{s}_evidences"] for s in COMPOSITION_SOURCES},
            )
            for i, r in enumerate(_fixture_rows())
            if r["food_name"] == TARGET_FOOD
        ]
        result = await get_composition_counts(
            _mock_session_sequence(rows, []),
            TARGET_FOOD,
            filter_source="+".join(subset),
        )
        assert result["data"]["no_concentration_count"] == len(sql_names), (
            f"subset={subset}: counts and rows disagree"
        )

    @pytest.mark.asyncio
    async def test_unfiltered_counts_include_every_source(self) -> None:
        """No filter_source means all three, not the two that predate PTFI."""
        rows = [
            _make_row(
                id=0,
                chemical_name="ptfi-only",
                chemical_classification=[],
                median_concentration=None,
                fdc_evidences=None,
                foodatlas_evidences=None,
                ptfi_evidences=[{"extraction": []}],
            )
        ]
        result = await get_composition_counts(_mock_session_sequence(rows, []), "x")
        assert result["data"]["no_concentration_count"] == 1
        assert result["data"]["total_row_count"] == 1

    @pytest.mark.asyncio
    async def test_total_row_count_is_pre_filter(self) -> None:
        result = await get_composition_counts(
            _counts_session(), TARGET_FOOD, filter_source="ptfi"
        )
        # Seven target rows regardless of how narrow the filter is — the
        # frontend subtracts metadata.total_rows from this to say how many
        # rows the filters are hiding.
        assert result["data"]["total_row_count"] == 7


class TestEverySourceIsEnumeratedEverywhere:
    """Adding a fourth source must fail loudly rather than half-work.

    Each of these places hand-listed fdc and foodatlas and was simply not
    revisited when PTFI arrived.
    """

    @pytest.mark.parametrize("source", COMPOSITION_SOURCES)
    def test_v1_composition_clause_covers_source(self, source: str) -> None:
        assert f"{source}_evidences" in _COMPOSITION_SELECT_CLAUSE
        assert f"'{source}'" in _COMPOSITION_SELECT_CLAUSE

    @pytest.mark.parametrize("source", COMPOSITION_SOURCES)
    def test_evidence_count_sort_covers_source(self, source: str) -> None:
        assert source in EVIDENCE_COUNT_SORT_SOURCES
        assert f"{source}_evidences" in VALID_SORT_COLS["evidence_count"]

    @pytest.mark.parametrize("source", COMPOSITION_SOURCES)
    def test_attestation_collection_covers_source(self, source: str) -> None:
        """Trust scoring must see every source's attestations."""
        row = dict.fromkeys((f"{s}_evidences" for s in COMPOSITION_SOURCES), None)
        row["chemical_name"] = "x"
        row[f"{source}_evidences"] = [{"extraction": [{"attestation_id": "a1"}]}]
        assert _collect_attestation_ids(row) == ["a1"]

    def test_resort_key_mirrors_the_sql_key(self) -> None:
        """The Python re-sort and the SQL ORDER BY sum the same columns."""
        # A row whose entire evidence count comes from one source must
        # outrank an empty row for every source in the SQL key.
        for s in EVIDENCE_COUNT_SORT_SOURCES:
            out = _resort_after_filter(
                [{"name": "empty"}, {"name": "has", f"{s}_evidences": [1, 2]}],
                "evidence_count",
                "DESC",
            )
            assert [r["name"] for r in out] == ["has", "empty"], (
                f"{s} is in the SQL sort key but not the Python one"
            )


class TestParseSources:
    def test_drops_unknown_and_retired_sources(self) -> None:
        assert parse_sources("fdc+dmd") == ["fdc"]
        assert parse_sources("bogus") == []
        assert parse_sources("+") == []

    def test_order_is_canonical_not_caller_supplied(self) -> None:
        assert parse_sources("ptfi+fdc") == parse_sources("fdc+ptfi") == ["fdc", "ptfi"]

    def test_deduplicates(self) -> None:
        assert parse_sources("fdc+fdc") == ["fdc"]


class TestRowHasSource:
    def test_missing_column_is_absent_not_an_error(self) -> None:
        # Not every query selects every evidence column.
        assert row_has_source({"fdc_evidences": [1]}, ("fdc", "ptfi"))
        assert not row_has_source({"fdc_evidences": None}, ("ptfi",))

    def test_empty_selection_matches_nothing(self) -> None:
        assert not row_has_source({"fdc_evidences": [1]}, ())
