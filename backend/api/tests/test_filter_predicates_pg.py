"""Filter predicates, executed against a real Postgres and compared to an oracle.

The composition source-filter bug (PR #294) had two properties that made
it survive to production, and this module is built to defeat both.

1. **The broken line was covered.** `test_multiple_sources` called
   `get_composition(filter_source="fdc+dmd")` against an `AsyncMock`, so
   `_build_query_parts` ran on every CI run and `if len(valid) == 1:` was
   an executed line in an 84%-covered file. Coverage never dipped. What
   was missing was any assertion about *which rows the generated SQL
   selects*. So every test here runs the production function against real
   data and compares the row set to a Python oracle over the same fixture.

2. **The rows path and the counts path drifted.** Two independent
   implementations of one predicate — SQL for the table, a Python loop
   for the sidebar facets — and nothing asserted they agreed. That
   mismatch is what the user saw: a pager promising 309 rows above an
   empty table. So every dimension here also asserts
   ``facet_count == len(rows under that filter)``.

Postgres rather than SQLite because the bioactivity filters are
``EXISTS (... jsonb_array_elements ... = ANY(:p))`` and ``text[] &&``,
which SQLite cannot parse at all. See tests/dbharness.py.
"""

from __future__ import annotations

import itertools
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from src.repositories._search_util import build_ilike_pattern
from src.repositories.bioactivity import (
    _source_kind_of,
    get_category_options,
    get_chemicals,
    get_evidence_type_counts,
    get_source_kind_counts,
)
from src.repositories.food import get_composition, get_composition_counts
from src.repositories.v1.entities import list_entities
from tests.dbharness import chem_bioactivity_row, measurement

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BIOACT = "anticancer"
OTHER_BIOACT = "antioxidant"

EVIDENCE_TYPES = ("in vitro", "in-vivo", "adme/tox")
SOURCE_KINDS = ("experimental", "predicted")
UNITS = ("uM", "nM")

# One chemical per (evidence_type, source_kind, unit) combination, so every
# single-dimension filter has a known-correct expected set and every pair
# has a known-correct intersection. 3 x 2 x 2 = 12 rows, plus fixtures for
# the edge cases each predicate has its own branch for.
_COMBOS = list(itertools.product(EVIDENCE_TYPES, SOURCE_KINDS, UNITS))


def _bioactivity_fixture() -> list[dict]:
    rows: list[dict] = []
    for i, (et, sk, unit) in enumerate(_COMBOS):
        rows.append(
            chem_bioactivity_row(
                bioactivity=BIOACT,
                chemical=f"chem{i:02d}",
                chem_id=f"c{i:02d}",
                measurements=[
                    measurement(
                        evidence_type=et,
                        evidence_source=sk,
                        unit=unit,
                        endpoint="IC50",
                        value=float(i + 1),
                    )
                ],
            )
        )
    # A row with two measurements that disagree on every dimension. Any
    # filter matching EITHER must keep it — the predicates are "row has at
    # least one matching measurement", not "all measurements match", and
    # an EXISTS rewritten as a scalar comparison would drop this.
    rows.append(
        chem_bioactivity_row(
            bioactivity=BIOACT,
            chemical="mixed",
            chem_id="cmix",
            measurements=[
                measurement(
                    evidence_type="in vitro", evidence_source="experimental", unit="uM"
                ),
                measurement(
                    evidence_type="adme/tox", evidence_source="predicted", unit="nM"
                ),
            ],
        )
    )
    # `computational` must classify as predicted: the SQL matches 'comp%'
    # and Python's _source_kind_of startswith("comp"). A twin that
    # implemented only 'pred%' would pass every other test here.
    rows.append(
        chem_bioactivity_row(
            bioactivity=BIOACT,
            chemical="computed",
            chem_id="ccomp",
            measurements=[
                measurement(
                    evidence_type="in vitro", evidence_source="computational", unit="uM"
                )
            ],
        )
    )
    # Unknown provenance — neither experimental nor predicted. Must be
    # excluded by both single-select branches, not silently bucketed.
    rows.append(
        chem_bioactivity_row(
            bioactivity=BIOACT,
            chemical="unknown-src",
            chem_id="cunk",
            measurements=[
                measurement(
                    evidence_type="in vitro", evidence_source="curated", unit="uM"
                )
            ],
        )
    )
    # Decoy under a different bioactivity. A predicate that loses its
    # parentheses ORs across `bioactivity_name = :name` and drags this in.
    rows.append(
        chem_bioactivity_row(
            bioactivity=OTHER_BIOACT,
            chemical="decoy",
            chem_id="cdecoy",
            measurements=[
                measurement(
                    evidence_type="in vitro", evidence_source="experimental", unit="uM"
                )
            ],
        )
    )
    return rows


# Chemical classifications, for the category filter (text[] && overlap).
_CATEGORIES = {
    "c00": ["flavonoid"],
    "c01": ["flavonoid", "polyphenol"],
    "c02": ["alkaloid"],
    "cmix": ["polyphenol"],
}


async def _load_bioactivity(session: AsyncSession) -> list[dict]:
    rows = _bioactivity_fixture()
    for r in rows:
        await session.execute(
            text(
                "INSERT INTO mv_chemical_bioactivity (bioactivity_name,"
                " bioactivity_foodatlas_id, chemical_name, chemical_foodatlas_id,"
                " measurement_count, active_count, inactive_count,"
                " unspecified_count, inconclusive_count, n_foods, measurements)"
                " VALUES (:bioactivity_name, :bioactivity_foodatlas_id,"
                " :chemical_name, :chemical_foodatlas_id, :measurement_count,"
                " :active_count, :inactive_count, :unspecified_count,"
                " :inconclusive_count, :n_foods, CAST(:measurements AS JSONB))"
            ),
            {**r, "measurements": _json(r["measurements"])},
        )
    for chem_id, classes in _CATEGORIES.items():
        await session.execute(
            text(
                "INSERT INTO mv_chemical_entities (foodatlas_id, common_name,"
                " chemical_classification) VALUES (:i, :n, :c)"
            ),
            {"i": chem_id, "n": chem_id, "c": classes},
        )
    await session.commit()
    return rows


def _json(value: object) -> str:
    return json.dumps(value)


def _names(payload: dict) -> set[str]:
    return {r["name"] for r in payload["data"]}


# --- oracles ----------------------------------------------------------
#
# Deliberately written against the fixture dicts, in the most literal way
# the English description of each filter allows. They must NOT reuse the
# production helpers, or a wrong helper would validate itself.


def _oracle_evidence_type(rows: Sequence[dict], selected: Sequence[str]) -> set[str]:
    want = set(selected)
    return {
        r["chemical_name"]
        for r in rows
        if r["bioactivity_name"] == BIOACT
        and any(m["evidence_type"] in want for m in r["measurements"])
    }


def _oracle_source_kind(rows: Sequence[dict], kind: str) -> set[str]:
    def kind_of(src: str) -> str:
        s = src.lower()
        if s.startswith("exp"):
            return "experimental"
        if s.startswith(("pred", "comp")):
            return "predicted"
        return ""

    return {
        r["chemical_name"]
        for r in rows
        if r["bioactivity_name"] == BIOACT
        and any(kind_of(m["evidence_source"]) == kind for m in r["measurements"])
    }


def _oracle_unit(rows: Sequence[dict], units: Sequence[str]) -> set[str]:
    want = set(units)
    return {
        r["chemical_name"]
        for r in rows
        if r["bioactivity_name"] == BIOACT
        and any(m["unit"] in want for m in r["measurements"])
    }


class TestBioactivityFilterPredicates:
    """Execute each bioactivity predicate; compare to the oracle."""

    @pytest.mark.parametrize("evidence_type", EVIDENCE_TYPES)
    async def test_evidence_type_single(
        self, pg_session: AsyncSession, evidence_type: str
    ) -> None:
        rows = await _load_bioactivity(pg_session)
        got = await get_chemicals(
            pg_session, BIOACT, rows_per_page=100, filter_evidence_type=evidence_type
        )
        assert _names(got) == _oracle_evidence_type(rows, [evidence_type])

    @pytest.mark.parametrize(
        "combo",
        [c for n in (2, 3) for c in itertools.combinations(EVIDENCE_TYPES, n)],
    )
    async def test_evidence_type_multiselect(
        self, pg_session: AsyncSession, combo: tuple[str, ...]
    ) -> None:
        """The arity that broke composition: 2-of-3 and 3-of-3."""
        rows = await _load_bioactivity(pg_session)
        got = await get_chemicals(
            pg_session,
            BIOACT,
            rows_per_page=100,
            filter_evidence_type="+".join(combo),
        )
        assert _names(got) == _oracle_evidence_type(rows, combo)

    @pytest.mark.parametrize("kind", SOURCE_KINDS)
    async def test_source_kind(self, pg_session: AsyncSession, kind: str) -> None:
        rows = await _load_bioactivity(pg_session)
        got = await get_chemicals(
            pg_session, BIOACT, rows_per_page=100, filter_source_kind=kind
        )
        assert _names(got) == _oracle_source_kind(rows, kind)

    async def test_computational_counts_as_predicted(
        self, pg_session: AsyncSession
    ) -> None:
        """SQL matches 'comp%'; the Python twin must agree."""
        await _load_bioactivity(pg_session)
        got = await get_chemicals(
            pg_session, BIOACT, rows_per_page=100, filter_source_kind="predicted"
        )
        assert "computed" in _names(got)
        assert _source_kind_of({"evidence_source": "computational"}) == "predicted"

    async def test_unknown_source_excluded_from_both_kinds(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_bioactivity(pg_session)
        for kind in SOURCE_KINDS:
            got = await get_chemicals(
                pg_session, BIOACT, rows_per_page=100, filter_source_kind=kind
            )
            assert "unknown-src" not in _names(got), kind

    @pytest.mark.parametrize("units", [("uM",), ("nM",), ("uM", "nM")])
    async def test_unit_multiselect(
        self, pg_session: AsyncSession, units: tuple[str, ...]
    ) -> None:
        rows = await _load_bioactivity(pg_session)
        got = await get_chemicals(
            pg_session, BIOACT, rows_per_page=100, filter_unit="+".join(units)
        )
        assert _names(got) == _oracle_unit(rows, units)

    async def test_category_array_overlap(self, pg_session: AsyncSession) -> None:
        """`text[] &&` — the other thing SQLite can't express."""
        await _load_bioactivity(pg_session)
        got = await get_chemicals(
            pg_session, BIOACT, rows_per_page=100, filter_category="flavonoid"
        )
        assert _names(got) == {"chem00", "chem01"}
        both = await get_chemicals(
            pg_session, BIOACT, rows_per_page=100, filter_category="flavonoid+alkaloid"
        )
        assert _names(both) == {"chem00", "chem01", "chem02"}

    async def test_row_with_one_matching_measurement_survives(
        self, pg_session: AsyncSession
    ) -> None:
        """EXISTS semantics: any matching measurement keeps the row."""
        await _load_bioactivity(pg_session)
        for et in ("in vitro", "adme/tox"):
            got = await get_chemicals(
                pg_session, BIOACT, rows_per_page=100, filter_evidence_type=et
            )
            assert "mixed" in _names(got), et

    async def test_no_predicate_leaks_across_bioactivities(
        self, pg_session: AsyncSession
    ) -> None:
        """A bare OR would bind across `bioactivity_name = :name`."""
        await _load_bioactivity(pg_session)
        for kwargs in (
            {"filter_evidence_type": "in vitro"},
            {"filter_evidence_type": "in vitro+adme/tox"},
            {"filter_source_kind": "experimental"},
            {"filter_unit": "uM+nM"},
            {"filter_category": "flavonoid"},
        ):
            got = await get_chemicals(pg_session, BIOACT, rows_per_page=100, **kwargs)
            assert "decoy" not in _names(got), kwargs

    async def test_filters_compose(self, pg_session: AsyncSession) -> None:
        """Two dimensions AND together rather than one silently winning."""
        rows = await _load_bioactivity(pg_session)
        got = await get_chemicals(
            pg_session,
            BIOACT,
            rows_per_page=100,
            filter_evidence_type="in vitro",
            filter_source_kind="experimental",
        )
        assert _names(got) == _oracle_evidence_type(rows, ["in vitro"]) & (
            _oracle_source_kind(rows, "experimental")
        )


class TestBioactivityCountsAgreeWithRows:
    """The drift guard: sidebar facet count == rows the filter returns.

    This is the invariant whose violation the user actually saw. It has to
    hold per dimension, or the sidebar promises rows the table can't show.
    """

    async def test_evidence_type_counts_match_rows(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_bioactivity(pg_session)
        facets = await get_evidence_type_counts(
            pg_session, BIOACT, "bioactivity-chemicals"
        )
        by_type = {f["evidence_type"]: f["count"] for f in facets["data"]}
        for et in EVIDENCE_TYPES:
            got = await get_chemicals(
                pg_session, BIOACT, rows_per_page=100, filter_evidence_type=et
            )
            assert by_type.get(et, 0) == got["metadata"]["total_rows"], et

    async def test_source_kind_counts_match_rows(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_bioactivity(pg_session)
        facets = await get_source_kind_counts(
            pg_session, BIOACT, "bioactivity-chemicals"
        )
        for kind in SOURCE_KINDS:
            got = await get_chemicals(
                pg_session, BIOACT, rows_per_page=100, filter_source_kind=kind
            )
            assert facets["data"][kind] == got["metadata"]["total_rows"], kind

    async def test_category_counts_match_rows(self, pg_session: AsyncSession) -> None:
        await _load_bioactivity(pg_session)
        facets = await get_category_options(pg_session, BIOACT)
        for entry in facets["data"]:
            cat = entry["category"]
            got = await get_chemicals(
                pg_session, BIOACT, rows_per_page=100, filter_category=cat
            )
            assert entry["count"] == got["metadata"]["total_rows"], cat


# --- composition ------------------------------------------------------


async def _load_composition(session: AsyncSession) -> list[dict]:
    """Composition rows covering every source subset, with classifications.

    test_composition_sources.py already proves the source predicate
    against SQLite. What it explicitly could not cover is the
    classification filter — `:c = ANY(chemical_classification)` and
    `chemical_classification = '{}'` need real arrays. That gap is closed
    here.
    """
    ev = [{"extraction": [{"attestation_id": "a1"}]}]
    rows: list[dict] = []
    combos = [
        ("fdc",),
        ("foodatlas",),
        ("ptfi",),
        ("fdc", "foodatlas"),
        ("fdc", "ptfi"),
        ("foodatlas", "ptfi"),
        ("fdc", "foodatlas", "ptfi"),
    ]
    classes = [["flavonoid"], ["alkaloid"], [], ["flavonoid", "alkaloid"]]
    for i, srcs in enumerate(combos):
        rows.append(
            {
                "food_name": "pepper",
                "chemical_name": f"x{i}",
                "chemical_foodatlas_id": f"e{i}",
                "chemical_classification": classes[i % len(classes)],
                "sources": srcs,
            }
        )
    for i, r in enumerate(rows):
        await session.execute(
            text(
                "INSERT INTO mv_food_chemical_composition (id, food_name,"
                " food_foodatlas_id, chemical_name, chemical_foodatlas_id,"
                " chemical_classification, median_concentration, fdc_evidences,"
                " foodatlas_evidences, ptfi_evidences, dmd_evidences)"
                " VALUES (:row_id, :food_name, 'f1', :chemical_name,"
                " :chemical_foodatlas_id, :cls, NULL,"
                " CAST(:fdc AS JSONB), CAST(:fa AS JSONB),"
                " CAST(:ptfi AS JSONB), NULL)"
            ),
            {
                "row_id": i,
                "food_name": r["food_name"],
                "chemical_name": r["chemical_name"],
                "chemical_foodatlas_id": r["chemical_foodatlas_id"],
                "cls": r["chemical_classification"],
                "fdc": _json(ev) if "fdc" in r["sources"] else None,
                "fa": _json(ev) if "foodatlas" in r["sources"] else None,
                "ptfi": _json(ev) if "ptfi" in r["sources"] else None,
            },
        )
    await session.commit()
    return rows


class TestCompositionClassificationPredicate:
    """The composition dimension SQLite could not execute."""

    @pytest.mark.parametrize("cls", ["flavonoid", "alkaloid"])
    async def test_named_classification(
        self, pg_session: AsyncSession, cls: str
    ) -> None:
        rows = await _load_composition(pg_session)
        got = await get_composition(
            pg_session, "pepper", rows_per_page=100, filter_classification=cls
        )
        want = {r["chemical_name"] for r in rows if cls in r["chemical_classification"]}
        assert {r["name"] for r in got["data"]} == want

    async def test_unclassified_bucket(self, pg_session: AsyncSession) -> None:
        """`n/a` selects the empty-array rows, not every row."""
        rows = await _load_composition(pg_session)
        got = await get_composition(
            pg_session, "pepper", rows_per_page=100, filter_classification="n/a"
        )
        want = {r["chemical_name"] for r in rows if not r["chemical_classification"]}
        assert {r["name"] for r in got["data"]} == want
        assert want, "fixture must contain an unclassified row"

    async def test_multiselect_is_a_union(self, pg_session: AsyncSession) -> None:
        rows = await _load_composition(pg_session)
        got = await get_composition(
            pg_session,
            "pepper",
            rows_per_page=100,
            filter_classification="flavonoid+alkaloid",
        )
        want = {
            r["chemical_name"]
            for r in rows
            if {"flavonoid", "alkaloid"} & set(r["chemical_classification"])
        }
        assert {r["name"] for r in got["data"]} == want

    async def test_class_counts_agree_with_rows(self, pg_session: AsyncSession) -> None:
        await _load_composition(pg_session)
        counts = await get_composition_counts(pg_session, "pepper")
        for cls, n in counts["data"]["classification_counts"].items():
            got = await get_composition(
                pg_session, "pepper", rows_per_page=100, filter_classification=cls
            )
            assert n == got["metadata"]["total_rows"], cls

    async def test_source_and_class_compose(self, pg_session: AsyncSession) -> None:
        rows = await _load_composition(pg_session)
        got = await get_composition(
            pg_session,
            "pepper",
            rows_per_page=100,
            filter_source="ptfi",
            filter_classification="flavonoid",
        )
        want = {
            r["chemical_name"]
            for r in rows
            if "ptfi" in r["sources"] and "flavonoid" in r["chemical_classification"]
        }
        assert {r["name"] for r in got["data"]} == want

    async def test_source_counts_agree_with_rows(
        self, pg_session: AsyncSession
    ) -> None:
        """The original bug, now executed against the real engine."""
        await _load_composition(pg_session)
        counts = await get_composition_counts(pg_session, "pepper")
        for src, n in counts["data"]["source_counts"].items():
            got = await get_composition(
                pg_session, "pepper", rows_per_page=100, filter_source=src
            )
            assert n == got["metadata"]["total_rows"], src

    @pytest.mark.parametrize(
        "combo",
        [
            c
            for n in (1, 2, 3)
            for c in itertools.combinations(("fdc", "foodatlas", "ptfi"), n)
        ],
    )
    async def test_multi_source_exact_set(
        self, pg_session: AsyncSession, combo: tuple[str, ...]
    ) -> None:
        """Exact row set per subset — NOT a bounds check.

        A `max(singles) <= joint <= sum(singles)` assertion is too weak to
        catch the original bug on a small fixture: with the predicate
        missing, `fdc+foodatlas` returns all 7 rows and 4 <= 7 <= 8 still
        holds. Bounds only bite when the unfiltered total exceeds the sum,
        which is a property of the data, not of the code. Comparing the
        actual set to the oracle bites always.
        """
        rows = await _load_composition(pg_session)
        got = await get_composition(
            pg_session, "pepper", rows_per_page=100, filter_source="+".join(combo)
        )
        want = {r["chemical_name"] for r in rows if set(combo) & set(r["sources"])}
        assert {r["name"] for r in got["data"]} == want, combo


class TestPaginationAgreesWithFiltering:
    """Paging must not lose, duplicate, or invent rows under a filter.

    The original bug was *experienced* through pagination: the pager said
    13 pages, page 1 rendered nothing. Even with a correct WHERE, a
    filter applied on the wrong side of LIMIT/OFFSET reproduces exactly
    that symptom, so the relationship between the two is worth pinning
    independently of the predicate itself.
    """

    @pytest.mark.parametrize("filter_source", ["", "ptfi", "fdc+ptfi"])
    async def test_pages_partition_the_filtered_set(
        self, pg_session: AsyncSession, filter_source: str
    ) -> None:
        await _load_composition(pg_session)
        first = await get_composition(
            pg_session, "pepper", rows_per_page=2, filter_source=filter_source
        )
        total = first["metadata"]["total_rows"]
        pages = first["metadata"]["total_pages"]

        seen: list[str] = []
        for page in range(1, pages + 1):
            got = await get_composition(
                pg_session,
                "pepper",
                page=page,
                rows_per_page=2,
                filter_source=filter_source,
            )
            seen.extend(r["name"] for r in got["data"])

        assert len(seen) == len(set(seen)), f"row served on two pages: {seen}"
        assert len(seen) == total, f"{len(seen)} rows across {pages} pages != {total}"

        unpaged = await get_composition(
            pg_session, "pepper", rows_per_page=100, filter_source=filter_source
        )
        assert set(seen) == {r["name"] for r in unpaged["data"]}

    async def test_total_pages_is_zero_when_nothing_matches(
        self, pg_session: AsyncSession
    ) -> None:
        """A pager promising pages over an empty table is the reported bug."""
        await _load_composition(pg_session)
        got = await get_composition(
            pg_session,
            "pepper",
            rows_per_page=2,
            filter_classification="nonexistent-class",
        )
        assert got["data"] == []
        assert got["metadata"]["total_rows"] == 0
        assert got["metadata"]["total_pages"] == 0

    async def test_no_page_is_empty_while_total_rows_is_positive(
        self, pg_session: AsyncSession
    ) -> None:
        """The contradiction invariant, server-side."""
        await _load_composition(pg_session)
        for src in ("fdc", "foodatlas", "ptfi", "fdc+ptfi", "foodatlas+ptfi"):
            head = await get_composition(
                pg_session, "pepper", rows_per_page=2, filter_source=src
            )
            if head["metadata"]["total_rows"] == 0:
                continue
            for page in range(1, head["metadata"]["total_pages"] + 1):
                got = await get_composition(
                    pg_session, "pepper", page=page, rows_per_page=2, filter_source=src
                )
                assert got["data"], f"{src} page {page} empty but total_rows > 0"


class TestEndpointUnitPair:
    """filter_endpoint only takes effect paired with filter_unit."""

    async def test_strict_pair_matches_both_fields(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_bioactivity(pg_session)
        await pg_session.execute(
            text(
                "INSERT INTO mv_chemical_bioactivity (bioactivity_name,"
                " chemical_name, chemical_foodatlas_id, measurement_count,"
                " measurements) VALUES (:b, 'pairfixture', 'cpair', 1,"
                " CAST(:m AS JSONB))"
            ),
            {
                "b": BIOACT,
                "m": json.dumps([measurement(endpoint="EC50", unit="mg/L", value=9.0)]),
            },
        )
        await pg_session.commit()

        got = await get_chemicals(
            pg_session,
            BIOACT,
            rows_per_page=100,
            filter_endpoint="EC50",
            filter_unit="mg/L",
        )
        assert _names(got) == {"pairfixture"}

        # Endpoint matches but unit does not -> the pair must not match.
        mismatched = await get_chemicals(
            pg_session,
            BIOACT,
            rows_per_page=100,
            filter_endpoint="EC50",
            filter_unit="uM",
        )
        assert "pairfixture" not in _names(mismatched)

    async def test_endpoint_without_unit_is_inert(
        self, pg_session: AsyncSession
    ) -> None:
        """Documented behaviour: endpoint alone applies no filter.

        Pinned deliberately. If it ever starts filtering, the sidebar
        counts (which assume the pair) silently stop matching the table.
        """
        await _load_bioactivity(pg_session)
        unfiltered = await get_chemicals(pg_session, BIOACT, rows_per_page=100)
        endpoint_only = await get_chemicals(
            pg_session, BIOACT, rows_per_page=100, filter_endpoint="IC50"
        )
        assert _names(endpoint_only) == _names(unfiltered)


@pytest.mark.filterwarnings("ignore::pytest.PytestWarning")
class TestEveryFilterDimensionIsExercised:
    """A new filter param must come with a predicate test, or CI fails.

    Pure source inspection — no DB, no event loop. The module-level
    `asyncio` mark does not apply to these, hence the local override.

    Every other test here guards a filter that exists today. This one
    guards the ones that don't exist yet: it reads the `filter_*` query
    parameters off the route signatures and asserts each is named in this
    module. Adding `filter_species` to a route and shipping it untested
    now breaks the build instead of waiting for a user to notice.

    Deliberately a source-text check rather than an import-and-inspect:
    it should fail for a filter that is *declared* on a route, whether or
    not the repository layer has caught up with it yet.
    """

    @staticmethod
    def _declared_filters() -> set[str]:
        routes = Path(__file__).resolve().parents[1] / "src" / "routes"
        pattern = re.compile(r"(filter_[a-z_]+)\s*:\s*str\s*=\s*Query")
        found: set[str] = set()
        for path in routes.rglob("*.py"):
            found.update(pattern.findall(path.read_text(encoding="utf-8")))
        return found

    def test_at_least_one_filter_is_discovered(self) -> None:
        """Guards the guard: a broken regex would vacuously pass below."""
        assert len(self._declared_filters()) >= 5

    def test_every_declared_filter_has_a_predicate_test(self) -> None:
        own_source = Path(__file__).read_text(encoding="utf-8")
        missing = sorted(
            f for f in self._declared_filters() if f"{f}=" not in own_source
        )
        assert not missing, (
            f"filter params with no executable predicate test: {missing}. "
            "Add a fixture + oracle here; a filter that only has route-level "
            "tests can return the wrong rows with every check green."
        )


class TestSortTwinsAgree:
    """The Python re-sort must order rows the same way the SQL does.

    `get_composition` sorts in SQL, then `_resort_after_filter` re-sorts in
    Python whenever trust != show_all, because the trust filter rewrites
    medians. Two implementations of one ordering. `evidence_count` had
    already drifted — it omitted ptfi, scoring every PTFI-only row 0 — and
    nothing covered the other two keys.
    """

    @pytest.mark.parametrize("sort_by", ["common_name", "median_concentration"])
    @pytest.mark.parametrize("sort_dir", ["asc", "desc"])
    async def test_sql_and_python_orderings_match(
        self, pg_session: AsyncSession, sort_by: str, sort_dir: str
    ) -> None:
        await _load_composition_with_medians(pg_session)
        # trust=show_all keeps the SQL order; default re-sorts in Python.
        sql_order = await get_composition(
            pg_session,
            "pepper",
            rows_per_page=100,
            sort_by=sort_by,
            sort_dir=sort_dir,
            trust="show_all",
        )
        py_order = await get_composition(
            pg_session,
            "pepper",
            rows_per_page=100,
            sort_by=sort_by,
            sort_dir=sort_dir,
            trust="default",
        )
        assert [r["name"] for r in sql_order["data"]] == [
            r["name"] for r in py_order["data"]
        ], f"{sort_by} {sort_dir}: SQL and Python orderings disagree"

    async def test_nulls_sort_last_in_both_paths(
        self, pg_session: AsyncSession
    ) -> None:
        """NULLS LAST in SQL; the Python twin appends the value-less rows."""
        await _load_composition_with_medians(pg_session)
        for trust in ("show_all", "default"):
            got = await get_composition(
                pg_session,
                "pepper",
                rows_per_page=100,
                sort_by="median_concentration",
                sort_dir="desc",
                trust=trust,
            )
            has_value = [r["median_concentration"] is not None for r in got["data"]]
            # Every True must precede every False.
            assert has_value == sorted(has_value, reverse=True), trust


async def _load_composition_with_medians(session: AsyncSession) -> None:
    """Composition rows with a spread of medians, including NULLs."""
    ev = [{"extraction": [{"attestation_id": "a1"}]}]
    values: list[float | None] = [5.0, 1.0, 3.0, None, 2.0, None, 4.0]
    for i, val in enumerate(values):
        await session.execute(
            text(
                "INSERT INTO mv_food_chemical_composition (id, food_name,"
                " food_foodatlas_id, chemical_name, chemical_foodatlas_id,"
                " chemical_classification, median_concentration,"
                " foodatlas_evidences) VALUES (:i, 'pepper', 'f1', :n, :cid,"
                " '{}', CAST(:mc AS JSONB), CAST(:ev AS JSONB))"
            ),
            {
                "i": i,
                "n": f"chem{i}",
                "cid": f"e{i}",
                "mc": None if val is None else json.dumps({"value": val}),
                "ev": json.dumps(ev),
            },
        )
    await session.commit()


class TestTrustFilterCountsAgree:
    """low_trust_count must describe the rows the toggle actually changes."""

    async def _load_with_trust(self, session: AsyncSession) -> None:
        rows = [("clean", "ok1", 0.9), ("dirty", "bad1", 0.1)]
        for i, (name, att, score) in enumerate(rows):
            await session.execute(
                text(
                    "INSERT INTO mv_food_chemical_composition (id, food_name,"
                    " food_foodatlas_id, chemical_name, chemical_foodatlas_id,"
                    " chemical_classification, median_concentration,"
                    " foodatlas_evidences) VALUES (:i, 'pepper', 'f1', :n,"
                    " :cid, '{}', NULL, CAST(:ev AS JSONB))"
                ),
                {
                    "i": i,
                    "n": name,
                    "cid": f"e{i}",
                    "ev": json.dumps([{"extraction": [{"attestation_id": att}]}]),
                },
            )
            await session.execute(
                text(
                    "INSERT INTO base_trust_signals (attestation_id,"
                    " signal_kind, score) VALUES (:a, 'llm_plausibility', :s)"
                ),
                {"a": att, "s": score},
            )
        await session.commit()

    async def test_show_all_is_a_superset_of_default(
        self, pg_session: AsyncSession
    ) -> None:
        await self._load_with_trust(pg_session)
        default = await get_composition(
            pg_session, "pepper", rows_per_page=100, trust="default"
        )
        show_all = await get_composition(
            pg_session, "pepper", rows_per_page=100, trust="show_all"
        )
        assert {r["name"] for r in default["data"]} <= {
            r["name"] for r in show_all["data"]
        }

    async def test_fully_low_trust_row_is_dropped_by_default(
        self, pg_session: AsyncSession
    ) -> None:
        await self._load_with_trust(pg_session)
        default = await get_composition(
            pg_session, "pepper", rows_per_page=100, trust="default"
        )
        show_all = await get_composition(
            pg_session, "pepper", rows_per_page=100, trust="show_all"
        )
        assert "dirty" not in {r["name"] for r in default["data"]}
        assert "dirty" in {r["name"] for r in show_all["data"]}

    async def test_low_trust_count_matches_the_rows_the_toggle_reveals(
        self, pg_session: AsyncSession
    ) -> None:
        await self._load_with_trust(pg_session)
        counts = await get_composition_counts(pg_session, "pepper")
        default = await get_composition(
            pg_session, "pepper", rows_per_page=100, trust="default"
        )
        show_all = await get_composition(
            pg_session, "pepper", rows_per_page=100, trust="show_all"
        )
        revealed = (
            show_all["metadata"]["total_rows"] - default["metadata"]["total_rows"]
        )
        # low_trust_count counts rows with AT LEAST ONE low-trust
        # extraction, which is >= the number the filter fully removes.
        assert counts["data"]["low_trust_count"] >= revealed
        assert counts["data"]["low_trust_count"] == 1


async def _load_food_entities(session: AsyncSession) -> None:
    """Foods whose names exercise ILIKE metacharacters."""
    rows = [
        ("f1", "tomato", ["fruit"]),
        ("f2", "Tomato Paste", ["processed"]),
        ("f3", "50% cocoa chocolate", ["processed"]),
        ("f4", "50g cocoa bar", ["processed"]),
        ("f5", "cocoa_nib", ["raw"]),
        ("f6", "cocoaXnib", ["raw"]),
    ]
    for fid, name, cls in rows:
        await session.execute(
            text(
                "INSERT INTO mv_food_entities (foodatlas_id, entity_type,"
                " common_name, scientific_name, synonyms, external_ids,"
                " food_classification, ambiguity_siblings) VALUES (:i,"
                " 'food', :n, '', '{}', '{}'::jsonb, :c, '[]'::jsonb)"
            ),
            {"i": fid, "n": name, "c": cls},
        )
    await session.commit()


class TestV1EntityFilters:
    """The public /v1 entity list — predicates never executed by a test."""

    async def test_classification_membership(self, pg_session: AsyncSession) -> None:
        await _load_food_entities(pg_session)
        rows, total = await list_entities(
            pg_session, "food", classification="processed", page_size=100
        )
        assert total == 3
        assert {r["common_name"] for r in rows} == {
            "Tomato Paste",
            "50% cocoa chocolate",
            "50g cocoa bar",
        }

    async def test_classification_and_query_compose(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_food_entities(pg_session)
        rows, total = await list_entities(
            pg_session, "food", q="cocoa", classification="raw", page_size=100
        )
        assert total == 2
        assert all("cocoa" in r["common_name"].lower() for r in rows)

    async def test_query_is_case_insensitive(self, pg_session: AsyncSession) -> None:
        """ILIKE, not LIKE — verified against the real collation."""
        await _load_food_entities(pg_session)
        _, total = await list_entities(pg_session, "food", q="TOMATO", page_size=100)
        assert total == 2

    async def test_pagination_partitions_the_filtered_set(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_food_entities(pg_session)
        _, total = await list_entities(pg_session, "food", page_size=100)
        seen: list[str] = []
        for page in range(1, (total + 1) // 2 + 2):
            rows, _ = await list_entities(pg_session, "food", page=page, page_size=2)
            seen.extend(r["common_name"] for r in rows)
        assert len(seen) == len(set(seen)) == total

    @pytest.mark.xfail(
        reason=(
            'v1 builds f"%{q}%" directly (repositories/v1/entities.py:70) '
            "instead of using build_ilike_pattern, so ILIKE metacharacters in "
            "user input are still wildcards. PR #289 fixed this for the "
            "internal search and did not reach /v1. Searching '50%' matches "
            "every name containing '50'. Flagged, not silently fixed: /v1 is "
            "the public API and widening or narrowing its match semantics is "
            "a product call."
        ),
        strict=True,
    )
    async def test_percent_in_query_is_a_literal(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_food_entities(pg_session)
        rows, total = await list_entities(pg_session, "food", q="50%", page_size=100)
        assert total == 1
        assert rows[0]["common_name"] == "50% cocoa chocolate"

    @pytest.mark.xfail(
        reason="Same gap as above: '_' is an ILIKE single-char wildcard.",
        strict=True,
    )
    async def test_underscore_in_query_is_a_literal(
        self, pg_session: AsyncSession
    ) -> None:
        await _load_food_entities(pg_session)
        _, total = await list_entities(pg_session, "food", q="cocoa_nib", page_size=100)
        assert total == 1


class TestIlikePatternAgainstRealPostgres:
    """build_ilike_pattern's escaping, executed rather than asserted on.

    tests/test_search_util.py checks the string it returns. Whether
    Postgres then treats that string as a literal is a different question
    — it depends on ILIKE's escape rules, which only the real engine
    knows.
    """

    @pytest.mark.parametrize(
        ("term", "expected"),
        [
            ("50%", {"50% cocoa chocolate"}),
            ("cocoa_nib", {"cocoa_nib"}),
            ("tomato", {"tomato", "Tomato Paste"}),
        ],
    )
    async def test_metacharacters_match_literally(
        self, pg_session: AsyncSession, term: str, expected: set[str]
    ) -> None:
        await _load_food_entities(pg_session)
        pattern = build_ilike_pattern(term)
        assert pattern is not None
        result = await pg_session.execute(
            text("SELECT common_name FROM mv_food_entities WHERE common_name ILIKE :q"),
            {"q": pattern},
        )
        assert {r[0] for r in result} == expected

    async def test_blank_input_yields_no_pattern(
        self, pg_session: AsyncSession
    ) -> None:
        """Whitespace-only input must not become ILIKE '% %'."""
        assert build_ilike_pattern("   ") is None
