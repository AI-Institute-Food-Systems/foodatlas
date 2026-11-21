"""Every bioactivity table x every Evidence/Unit/Source combination, on Postgres.

For each table (bioactivity → chemicals/foods, chemical → bioactivities,
food → bioactivities, food → inferred bioactivities) and each filter
combination, the API must agree with the oracle in ``bioact_contract`` on:

- which rows are listed and the table total;
- each row's "N assays" button, which must equal the modal total;
- every sidebar facet count (Evidence, Unit, Source, Category).

Since the facets and the rows each equal the oracle, every facet count also
equals the table total after picking that value. ``/measurements`` must
return exactly the contract's ``api_measurements``, which is what the
frontend modal tests feed the modal.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import text
from src.repositories import bioactivity
from src.repositories.bioactivity_facets import (
    PageFilters,
    get_category_counts,
    get_evidence_type_counts,
    get_source_kind_counts,
    get_unit_counts,
)
from tests.bioactfix import chem_bioactivity_row, food_bioactivity_row, load_bioactivity

from tests import bioact_contract as oracle

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DATA = oracle.load_contract()
CAP = 25

# (direction, page entity, search term that hits some rows)
PAGES = [
    ("bioactivity-chemicals", "anticancer", "chem0"),
    ("bioactivity-chemicals", "antioxidant", "chem1"),
    ("bioactivity-foods", "anticancer", "app"),
    ("bioactivity-foods", "antioxidant", "pea"),
    ("chemical-bioactivities", "chem01", "cancer"),
    ("chemical-bioactivities", "chem05", "oxid"),
    ("food-bioactivities", "apple", "cancer"),
    ("food-bioactivities", "pear", "oxid"),
    ("food-inferred-bioactivities", "apple", "chem05"),
    ("food-inferred-bioactivities", "pear", "cancer"),
]
CATEGORIES = ["", "flavonoid", "flavonoid+alkaloid"]


def _cid(name: str) -> str:
    return f"c-{name}"


def _fid(name: str) -> str:
    return f"f-{name}"


async def _load(session: AsyncSession) -> None:
    chem_rows, food_rows = [], []
    for p in DATA["pairs"]:
        ms = p["measurements"]
        common = {
            "bioactivity": p["bioactivity"],
            "measurements": ms[:CAP],
            "hidden": ms[CAP:],
        }
        if p["kind"] == "chemical":
            chem_rows.append(
                chem_bioactivity_row(
                    chemical=p["head"], chem_id=_cid(p["head"]), **common
                )
            )
        else:
            food_rows.append(
                food_bioactivity_row(food=p["head"], food_id=_fid(p["head"]), **common)
            )
    await load_bioactivity(session, chem_rows)
    await load_bioactivity(session, food_rows, mv="mv_food_bioactivity")
    for chem, cats in DATA["categories"].items():
        await session.execute(
            text(
                "INSERT INTO mv_chemical_entities (foodatlas_id, common_name,"
                " chemical_classification) VALUES (:i, :n, :c)"
            ),
            {"i": _cid(chem), "n": chem, "c": cats},
        )
    for food, chems in DATA["composition"].items():
        for chem in chems:
            await session.execute(
                text(
                    "INSERT INTO mv_food_chemical_composition (food_name,"
                    " food_foodatlas_id, chemical_name, chemical_foodatlas_id)"
                    " VALUES (:f, :fi, :c, :ci)"
                ),
                {"f": food, "fi": _fid(food), "c": chem, "ci": _cid(chem)},
            )
    await session.commit()


# --- oracle rows ---------------------------------------------------------


def _scope(direction: str, page: str) -> list[tuple[Any, str, list[dict], str]]:
    """(row key, searchable text, measurements, chemical or '') per table row."""
    pairs = DATA["pairs"]
    if direction == "food-inferred-bioactivities":
        chems = DATA["composition"][page]
        return [
            (
                (p["head"], p["bioactivity"]),
                f"{p['bioactivity']}\n{p['head']}",
                p["measurements"],
                p["head"],
            )
            for p in pairs
            if p["kind"] == "chemical" and p["head"] in chems
        ]
    kind, pivot = {
        "bioactivity-chemicals": ("chemical", "bioactivity"),
        "bioactivity-foods": ("food", "bioactivity"),
        "chemical-bioactivities": ("chemical", "head"),
        "food-bioactivities": ("food", "head"),
    }[direction]
    out = []
    for p in pairs:
        if p["kind"] != kind or p[pivot] != page:
            continue
        name = p["head"] if pivot == "bioactivity" else p["bioactivity"]
        chem = p["head"] if kind == "chemical" else ""
        out.append((name, name, p["measurements"], chem))
    return out


def _listed(direction: str, page: str, f: dict, skip: str = "") -> list[tuple]:
    rows = []
    for key, searchable, ms, chem in _scope(direction, page):
        if f["search"] and not any(
            f["search"].lower() in part.lower() for part in searchable.split("\n")
        ):
            continue
        cats = oracle_cats(f) if direction == "bioactivity-chemicals" else []
        if (
            cats
            and skip != "category"
            and not set(cats) & set(DATA["categories"].get(chem, []))
        ):
            continue
        rows.append((key, ms, chem))
    return rows


def oracle_cats(f: dict) -> list[str]:
    return [c for c in f["category"].split("+") if c]


def _expected(direction: str, page: str, f: dict) -> dict:
    listed = [
        (k, ms)
        for k, ms, _ in _listed(direction, page, f)
        if any(oracle.matches(m, f) for m in ms)
    ]
    exp: dict = {
        "rows": {k: oracle.modal_count(ms, f) for k, ms in listed},
        "evidence": {},
        "unit": {},
        "category": {},
    }
    for dim, label in (
        ("evidence", lambda m: (m.get("evidence_type") or "").strip()),
        ("unit", lambda m: oracle.norm_unit(m.get("unit"))),
    ):
        counts: dict[str, int] = {}
        for _, ms, _chem in _listed(direction, page, f):
            for v in {label(m) for m in ms if oracle.matches(m, f, skip=dim)} - {""}:
                counts[v] = counts.get(v, 0) + 1
        exp[dim] = counts
    src_rows = [ms for _, ms, _ in _listed(direction, page, f)]
    exp["source"] = {
        "both": sum(any(oracle.matches(m, f, "source") for m in ms) for ms in src_rows),
        **{
            sk: sum(
                any(oracle.matches(m, {**f, "source": sk}) for m in ms)
                for ms in src_rows
            )
            for sk in ("experimental", "predicted")
        },
    }
    if direction == "bioactivity-chemicals":
        for _, ms, chem in _listed(direction, page, f, skip="category"):
            if any(oracle.matches(m, f) for m in ms):
                for c in DATA["categories"].get(chem, []):
                    exp["category"][c] = exp["category"].get(c, 0) + 1
    return exp


# --- API -----------------------------------------------------------------


async def _api(session: AsyncSession, direction: str, page: str, f: dict) -> dict:
    kw = {
        "search": f["search"],
        "filter_evidence_type": "+".join(f["evidence"]),
        "filter_unit": "+".join(f["unit"]),
        "filter_source_kind": f["source"],
    }
    if direction == "food-inferred-bioactivities":
        payload = await bioactivity.get_food_inferred_bioactivities(
            session, page, rows_per_page=1000, **kw
        )
        rows = {
            (r["chemical"], r["bioactivity"]): r["measurement_count"]
            for r in payload["data"]
        }
    else:
        fetch = {
            "bioactivity-chemicals": bioactivity.get_chemicals,
            "bioactivity-foods": bioactivity.get_foods,
            "chemical-bioactivities": bioactivity.get_chemical_bioactivities,
            "food-bioactivities": bioactivity.get_food_bioactivities,
        }[direction]
        extra = (
            {"filter_category": f["category"]}
            if direction == "bioactivity-chemicals"
            else {}
        )
        payload = await fetch(session, page, rows_per_page=1000, **kw, **extra)
        rows = {r["name"]: r["measurement_count"] for r in payload["data"]}
    assert payload["metadata"]["total_rows"] == len(rows)
    pf = PageFilters(
        search=f["search"],
        evidence_type=kw["filter_evidence_type"],
        unit=kw["filter_unit"],
        source_kind=f["source"],
        category=f["category"],
    )
    ev = await get_evidence_type_counts(session, page, direction, pf)
    un = await get_unit_counts(session, page, direction, pf)
    src = await get_source_kind_counts(session, page, direction, pf)
    got = {
        "rows": rows,
        "evidence": {d["evidence_type"]: d["count"] for d in _data(ev)},
        "unit": {d["unit"]: d["count"] for d in _data(un)},
        "source": src["data"],
        "category": {},
    }
    if direction == "bioactivity-chemicals":
        cat = await get_category_counts(session, page, pf)
        got["category"] = {d["category"]: d["count"] for d in _data(cat)}
    return got


def _data(payload: dict[str, object]) -> list[dict]:
    data = payload["data"]
    assert isinstance(data, list)
    return data


def _key(m: dict, fields: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(m.get(k)) for k in fields)


def _cases(direction: str, search_term: str) -> list[dict]:
    base = [{**c, "search": "", "category": ""} for c in oracle.filter_cases()]
    cats = CATEGORIES if direction == "bioactivity-chemicals" else [""]
    extra = [
        {**c, "search": s, "category": cat}
        for c in oracle.filter_cases()[::23]
        for s in ("", search_term)
        for cat in cats
        if s or cat
    ]
    return base + extra


@pytest.mark.parametrize(("direction", "page", "term"), PAGES)
async def test_every_filter_combination(
    pg_session: AsyncSession, direction: str, page: str, term: str
) -> None:
    await _load(pg_session)
    for f in _cases(direction, term):
        assert await _api(pg_session, direction, page, f) == _expected(
            direction, page, f
        ), f


async def test_measurements_endpoint_matches_contract(pg_session: AsyncSession) -> None:
    await _load(pg_session)
    fields = (
        "evidence_type",
        "evidence_source",
        "endpoint",
        "unit",
        "value",
        "outcome",
    )
    for p in DATA["pairs"]:
        head = _cid(p["head"]) if p["kind"] == "chemical" else _fid(p["head"])
        rel = "r6" if p["kind"] == "chemical" else "r5"
        got = await bioactivity.get_measurements(
            pg_session, head, f"b-{p['bioactivity']}", rel
        )
        want = p["api_measurements"]
        assert sorted(_key(m, fields) for m in _data(got)) == sorted(
            _key(m, fields) for m in want
        )


async def test_contract_file_is_current() -> None:
    """The committed contract must match what the oracle derives today."""
    fresh = oracle.build_dataset()
    for p in fresh["pairs"]:
        p["api_measurements"] = oracle.api_measurements(p["measurements"])
    fresh["modal_filters"], fresh["modal_cases"] = oracle.modal_cases(fresh)
    assert fresh == DATA, "run: uv run python -m tests.bioact_contract"
