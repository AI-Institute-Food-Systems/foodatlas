"""One user story that exercises 100% of the public /v1 API, live.

Story: a developer builds "what in garlic helps with hypertension?" —
search → food → its chemicals → one chemical → its foods / diseases /
bioactivities / taxonomy → a disease → its chemicals / taxonomy → a
bioactivity → its chemicals / foods → the triplets behind one edge → one
attestation → stats and bundles.  Every operation in the live OpenAPI
document must be hit, and every parameter must be sent with a valid, a
boundary and an invalid value.  Coverage is asserted at the end.

Not a pytest: it needs a real deployment with data, and ~300 requests.

Usage (from backend/api):
    uv run python scripts/v1_story.py                 # staging, key from frontend/.env.local
    V1_STORY_BASE=http://localhost:8000 V1_STORY_KEY=... uv run python scripts/v1_story.py

Writes results.md (coverage, failures, per-operation matrix) and
requests.log (every request: status, latency, url) to
~/foodatlas-v1-story/<timestamp>/ and prints the summary. Exit 1 on any
FAIL or uncovered operation/parameter.
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import httpx

_REPO = Path(__file__).resolve().parents[3]


def _env(name: str) -> str:
    text = (_REPO / "frontend" / ".env.local").read_text()
    match = re.search(rf'^{name}="?([^"\n]+)', text, re.M)
    if not match:
        raise SystemExit(f"{name} not in frontend/.env.local and not overridden")
    return match.group(1)


BASE = os.environ.get("V1_STORY_BASE") or _env("NEXT_PUBLIC_API_URL")
KEY = os.environ.get("V1_STORY_KEY") or _env("API_KEY")
OUT = Path.home() / "foodatlas-v1-story" / time.strftime("%Y-%m-%d_%H%M%S")
OUT.mkdir(parents=True, exist_ok=True)
LOG = (OUT / "requests.log").open("w")

client = httpx.Client(base_url=BASE, timeout=60)
results: list[dict] = []  # {op, check, status, detail}
hits: dict[str, set[str]] = defaultdict(set)  # op -> query params sent
BIG = 99999999999999999999


def get(
    path: str, params: dict | None = None, *, auth: str | None = KEY
) -> httpx.Response:
    headers = {"Authorization": f"Bearer {auth}"} if auth is not None else {}
    t = time.perf_counter()
    r = client.get(path, params=params, headers=headers)
    ms = int((time.perf_counter() - t) * 1000)
    LOG.write(f"{r.status_code} {ms:5d}ms {r.request.url}\n")
    time.sleep(0.05)
    return r


def op_of(path: str) -> str:
    """Map a concrete path to its OpenAPI template."""
    for tpl in OPS:
        if re.fullmatch(re.sub(r"\{[^}]+\}", r"[^/]+", tpl), path):
            return tpl
    raise KeyError(path)


def check(op: str, name: str, ok: bool, detail: str = "") -> bool:
    results.append(
        {"op": op, "check": name, "status": "PASS" if ok else "FAIL", "detail": detail}
    )
    return ok


def call(
    path: str, params: dict | None = None, *, auth: str | None = KEY
) -> httpx.Response:
    r = get(path, params, auth=auth)
    hits[op_of(path)].update((params or {}).keys())
    return r


# ---------------------------------------------------------------- schema
SPEC = client.get("/openapi.json").json()
OPS = {p: m["get"] for p, m in SPEC["paths"].items()}
SCHEMAS = SPEC["components"]["schemas"]


def resolve(s: dict) -> dict:
    while "$ref" in s:
        s = SCHEMAS[s["$ref"].rsplit("/", 1)[1]]
    return s


def validate(s: dict, v: object, path: str = "$") -> list[str]:
    """Minimal JSON-schema walk: type / required / properties / items / anyOf."""
    s = resolve(s)
    errs: list[str] = []
    if "anyOf" in s:
        if not any(not validate(o, v, path) for o in s["anyOf"]):
            errs.append(f"{path}: matches no anyOf branch")
        return errs
    t = s.get("type")
    if t == "object":
        if not isinstance(v, dict):
            return [f"{path}: expected object"]
        for k in s.get("required", []):
            if k not in v:
                errs.append(f"{path}.{k}: missing")
        for k, sub in s.get("properties", {}).items():
            if k in v:
                errs += validate(sub, v[k], f"{path}.{k}")
    elif t == "array":
        if not isinstance(v, list):
            return [f"{path}: expected array"]
        for i, item in enumerate(v[:5]):
            errs += validate(s["items"], item, f"{path}[{i}]")
    elif t == "string" and not isinstance(v, str):
        errs.append(f"{path}: expected string, got {type(v).__name__}")
    elif t == "integer" and not (isinstance(v, int) and not isinstance(v, bool)):
        errs.append(f"{path}: expected integer, got {type(v).__name__}")
    elif t == "number" and not isinstance(v, (int, float)):
        errs.append(f"{path}: expected number, got {type(v).__name__}")
    elif t == "boolean" and not isinstance(v, bool):
        errs.append(f"{path}: expected boolean")
    return errs


def ok200(
    path: str, params: dict | None = None, label: str = "200 + schema"
) -> dict | None:
    op = op_of(path)
    r = call(path, params)
    if not check(
        op,
        label,
        r.status_code == 200,
        f"{r.status_code} {r.request.url.query.decode()}",
    ):
        return None
    body = r.json()
    schema = OPS[op]["responses"]["200"]["content"]["application/json"]["schema"]
    errs = validate(schema, body)
    check(op, f"{label} shape", not errs, "; ".join(errs[:3]))
    return body


# ---------------------------------------------------------------- generic suites
def auth_suite(path: str) -> None:
    op = op_of(path)
    check(op, "no key → 401", call(path, auth=None).status_code == 401)
    check(op, "bogus key → 401", call(path, auth="not-a-key").status_code == 401)


def list_suite(path: str, base: dict | None = None) -> list[dict]:
    """Pagination contract for a list endpoint. Returns page-1 rows."""
    op = op_of(path)
    base = dict(base or {})
    p1 = ok200(path, base) or {}
    rows, page = p1.get("data", []), p1.get("page", {})
    total = page.get("total", 0)
    check(
        op,
        "has_more consistent",
        page.get("has_more") == (total > len(rows)),
        f"total={total} rows={len(rows)}",
    )
    one = ok200(path, {**base, "page_size": 1}, "page_size=1") or {}
    check(op, "page_size=1 → 1 row", len(one.get("data", [])) == min(1, total))
    last = 1
    if total > 1:
        p2 = ok200(path, {**base, "page": 2, "page_size": 1}, "page=2") or {}
        ids1 = json.dumps(one.get("data"), sort_keys=True)
        ids2 = json.dumps(p2.get("data"), sort_keys=True)
        check(op, "page 2 ≠ page 1", ids1 != ids2)
        last = math.ceil(total / 100)
        lp = ok200(path, {**base, "page": last, "page_size": 100}, "last page") or {}
        check(
            op, "last page has_more=false", lp.get("page", {}).get("has_more") is False
        )
        check(
            op,
            "last page rows = total mod",
            len(lp.get("data", [])) == (total - (last - 1) * 100),
        )
    r = call(path, {**base, "page": (last + 1) if total > 1 else 2, "page_size": 100})
    check(
        op,
        "page beyond end → 200 empty",
        r.status_code == 200 and r.json()["data"] == [],
    )
    for bad in (
        {"page": 0},
        {"page": -1},
        {"page": "abc"},
        {"page_size": 0},
        {"page_size": 101},
        {"page_size": "x"},
    ):
        r = call(path, {**base, **bad})
        check(op, f"{bad} → 422", r.status_code == 422, str(r.status_code))
    r = call(path, {**base, "page": BIG})
    check(op, "page=huge → 4xx not 500", r.status_code < 500, str(r.status_code))
    return rows


def filter_suite(
    path: str,
    param: str,
    good: str,
    bad: str | None,
    predicate=None,
    base: dict | None = None,
    *,
    closed: bool = True,
) -> None:
    """A filter param with a valid value (rows, all satisfying it) and an
    invalid one. Closed vocabularies must 422; open ones (ids, free text,
    data-driven labels) may answer 200-empty instead."""
    op = op_of(path)
    b = dict(base or {})
    body = ok200(path, {**b, param: good}, f"{param}={good}") or {}
    rows = body.get("data", [])
    check(
        op,
        f"{param}={good} → rows",
        len(rows) > 0,
        f"total={body.get('page', {}).get('total')}",
    )
    if predicate and rows:
        check(
            op,
            f"{param}={good} rows all satisfy filter",
            all(predicate(x) for x in rows),
        )
    if bad is None:
        return
    r = call(path, {**b, param: bad})
    total = r.json().get("page", {}).get("total") if r.status_code == 200 else None
    if closed:
        check(
            op,
            f"{param}={bad} → 422",
            r.status_code == 422,
            f"{r.status_code} total={total}",
        )
    else:
        check(
            op,
            f"{param}={bad} → 422 or 200-empty",
            r.status_code == 422 or (r.status_code == 200 and total == 0),
            f"{r.status_code} total={total}",
        )


def detail_404(path_tpl: str, bad_id: str = "e999999999") -> None:
    op = op_of(path_tpl.format(id=bad_id))
    r = call(path_tpl.format(id=bad_id))
    check(op, "unknown id → 404", r.status_code == 404, str(r.status_code))


# ---------------------------------------------------------------- the story
def story() -> None:
    # 0. Am I allowed in?  Is the API alive?
    check(
        "/health",
        "unauthenticated /health → 200",
        client.get("/health").status_code == 200,
    )
    auth_suite("/v1/stats")
    stats = ok200("/v1/stats") or {}

    # 1. Search "garlic" across everything, then per entity type.
    auth_suite("/v1/search")
    hits_all = list_suite("/v1/search", {"q": "garlic"})
    check(
        "/v1/search",
        "q required → 422 when missing",
        call("/v1/search").status_code == 422,
    )
    for et in ("food", "chemical", "disease", "bioactivity"):
        filter_suite(
            "/v1/search",
            "entity_type",
            et,
            "Food",
            lambda x, et=et: x.get("entity_type") == et,
            {"q": "a"},
        )
    for q in ("%", "_", "'", "\\", "(", "garlić", " " * 3, "x" * 500, "e1595"):
        r = call("/v1/search", {"q": q})
        check(
            "/v1/search",
            f"q={q[:6]!r} never 500",
            r.status_code < 500,
            str(r.status_code),
        )
    food_hit = next((h for h in hits_all if h.get("entity_type") == "food"), None)
    check(
        "/v1/search",
        "garlic search returns a food",
        food_hit is not None,
        str({h.get("entity_type") for h in hits_all}),
    )
    food_id = food_hit["id"] if food_hit else "e1595"

    # 2. Foods: list, filter, detail, taxonomy, sub-resources.
    auth_suite("/v1/foods")
    foods = list_suite("/v1/foods")
    filter_suite(
        "/v1/foods", "q", "garlic", None, lambda x: "garlic" in x["common_name"].lower()
    )
    cls = next((c for f in foods for c in f.get("food_classification", [])), None)
    if cls:
        filter_suite(
            "/v1/foods",
            "classification",
            cls,
            "no-such-class",
            lambda x: cls in x["food_classification"],
            closed=False,
        )
    auth_suite(f"/v1/foods/{food_id}")
    ok200(f"/v1/foods/{food_id}")
    detail_404("/v1/foods/{id}")
    bad = [
        i
        for i in [f["id"] for f in foods[:10]]
        if call(f"/v1/foods/{i}").status_code != 200
    ]
    check(
        "/v1/foods/{food_id}",
        "first 10 listed foods all 200 on detail",
        not bad,
        f"failing: {bad}",
    )
    auth_suite(f"/v1/foods/{food_id}/taxonomy")
    ok200(f"/v1/foods/{food_id}/taxonomy")
    detail_404("/v1/foods/{id}/taxonomy")
    auth_suite(f"/v1/foods/{food_id}/chemicals")
    fchems = list_suite(f"/v1/foods/{food_id}/chemicals")
    detail_404("/v1/foods/{id}/chemicals")
    auth_suite(f"/v1/foods/{food_id}/bioactivities")
    list_suite(f"/v1/foods/{food_id}/bioactivities")
    detail_404("/v1/foods/{id}/bioactivities")

    # 3. Chemicals: the developer searches quercetin, walks everything on it.
    qhit = (
        ok200(
            "/v1/search",
            {"q": "quercetin", "entity_type": "chemical"},
            "q=quercetin chemical",
        )
        or {}
    ).get("data", [])
    chem_id = next(
        (h["id"] for h in qhit if h["common_name"].lower() == "quercetin"),
        fchems[0]["chemical_id"] if fchems else "e60502",
    )
    auth_suite("/v1/chemicals")
    chems = list_suite("/v1/chemicals")
    filter_suite(
        "/v1/chemicals",
        "q",
        "quercetin",
        None,
        lambda x: "quercetin" in x["common_name"].lower(),
    )
    ccls = next(
        (c for ch in chems for c in ch.get("chemical_classification", [])), None
    )
    if ccls:
        filter_suite(
            "/v1/chemicals",
            "classification",
            ccls,
            "no-such-class",
            lambda x: ccls in x["chemical_classification"],
            closed=False,
        )
    auth_suite(f"/v1/chemicals/{chem_id}")
    ok200(f"/v1/chemicals/{chem_id}")
    bad = [
        i
        for i in [c["id"] for c in chems[:10]]
        if call(f"/v1/chemicals/{i}").status_code != 200
    ]
    check(
        "/v1/chemicals/{chemical_id}",
        "first 10 listed chemicals all 200 on detail",
        not bad,
        f"failing: {bad}",
    )
    detail_404("/v1/chemicals/{id}")
    auth_suite(f"/v1/chemicals/{chem_id}/taxonomy")
    ok200(f"/v1/chemicals/{chem_id}/taxonomy")
    detail_404("/v1/chemicals/{id}/taxonomy")
    auth_suite(f"/v1/chemicals/{chem_id}/foods")
    list_suite(f"/v1/chemicals/{chem_id}/foods")
    c0 = fchems[0]["chemical_id"] if fchems else chem_id
    tri = (
        ok200(
            "/v1/triplets",
            {"head_id": food_id, "tail_id": c0, "relationship": "contains"},
            "composition row ⇒ contains-triplet",
        )
        or {}
    )
    check(
        "/v1/chemicals/{chemical_id}/foods",
        "reciprocal: food composition row has a contains-triplet",
        (tri.get("page", {}).get("total") or 0) > 0,
        f"{food_id}→{c0} total={tri.get('page', {}).get('total')}",
    )
    detail_404("/v1/chemicals/{id}/foods")
    auth_suite(f"/v1/chemicals/{chem_id}/diseases")
    cdis = list_suite(f"/v1/chemicals/{chem_id}/diseases")
    for rel in ("reduces", "worsens"):
        filter_suite(
            f"/v1/chemicals/{chem_id}/diseases",
            "relation",
            rel,
            "improves",
            lambda x, rel=rel: x.get("relation") == rel,
        )
    detail_404("/v1/chemicals/{id}/diseases")
    auth_suite(f"/v1/chemicals/{chem_id}/bioactivities")
    cbio = list_suite(f"/v1/chemicals/{chem_id}/bioactivities")
    detail_404("/v1/chemicals/{id}/bioactivities")

    # 4. Diseases: the one the chemical reduces.
    dis_id = next(
        (d["disease_id"] for d in cdis if d.get("relation") == "reduces"),
        cdis[0]["disease_id"] if cdis else "e30",
    )
    auth_suite("/v1/diseases")
    list_suite("/v1/diseases")
    filter_suite(
        "/v1/diseases",
        "q",
        "hypertension",
        None,
        lambda x: "hypertension" in x["common_name"].lower(),
    )
    auth_suite(f"/v1/diseases/{dis_id}")
    ok200(f"/v1/diseases/{dis_id}")
    detail_404("/v1/diseases/{id}")
    auth_suite(f"/v1/diseases/{dis_id}/taxonomy")
    ok200(f"/v1/diseases/{dis_id}/taxonomy")
    detail_404("/v1/diseases/{id}/taxonomy")
    auth_suite(f"/v1/diseases/{dis_id}/chemicals")
    dchems = list_suite(f"/v1/diseases/{dis_id}/chemicals")
    for rel in ("reduces", "worsens"):
        filter_suite(
            f"/v1/diseases/{dis_id}/chemicals",
            "relation",
            rel,
            "bogus",
            lambda x, rel=rel: x.get("relation") == rel,
        )
    tri = (
        ok200(
            "/v1/triplets",
            {"head_id": chem_id, "tail_id": dis_id, "relationship": "reduces"},
            "correlation row ⇒ reduces-triplet",
        )
        or {}
    )
    check(
        "/v1/diseases/{disease_id}/chemicals",
        "reciprocal: chem→disease correlation row has a reduces-triplet",
        (tri.get("page", {}).get("total") or 0) > 0,
        f"{chem_id}→{dis_id} total={tri.get('page', {}).get('total')}",
    )
    # View grain is one row per (chemical, source chemical); rows must be
    # distinguishable by that pair. Whether the endpoint should ALSO
    # collapse to one row per chemical (as the website now does) is an
    # open contract decision — reported separately so it cannot hide.
    pairs = [(c["chemical_id"], c.get("source_chemical_id")) for c in dchems]
    check(
        "/v1/diseases/{disease_id}/chemicals",
        "no duplicate (chemical, source) rows on page 1",
        len(pairs) == len(set(pairs)),
        f"{len(pairs) - len(set(pairs))} dupes",
    )
    ids = [c["chemical_id"] for c in dchems]
    check(
        "/v1/diseases/{disease_id}/chemicals",
        "OPEN DECISION: one row per chemical on page 1",
        len(ids) == len(set(ids)),
        f"{len(ids) - len(set(ids))} repeats (view grain, see source_chemical_id)",
    )
    detail_404("/v1/diseases/{id}/chemicals")

    # 5. Bioactivities: one the chemical exhibits.
    bio_id = cbio[0]["bioactivity_id"] if cbio else "e1"
    auth_suite("/v1/bioactivities")
    list_suite("/v1/bioactivities")
    filter_suite(
        "/v1/bioactivities",
        "q",
        "antioxidant",
        None,
        lambda x: "antioxidant" in x["common_name"].lower(),
    )
    auth_suite(f"/v1/bioactivities/{bio_id}")
    bio = (ok200(f"/v1/bioactivities/{bio_id}") or {}).get("data", {})
    detail_404("/v1/bioactivities/{id}")
    auth_suite(f"/v1/bioactivities/{bio_id}/chemicals")
    bchem = ok200(f"/v1/bioactivities/{bio_id}/chemicals") or {}
    list_suite(f"/v1/bioactivities/{bio_id}/chemicals")
    check(
        "/v1/bioactivities/{bioactivity_id}/chemicals",
        "total = detail.n_chemicals",
        bchem.get("page", {}).get("total") == bio.get("n_chemicals"),
        f"{bchem.get('page', {}).get('total')} vs {bio.get('n_chemicals')}",
    )
    detail_404("/v1/bioactivities/{id}/chemicals")
    auth_suite(f"/v1/bioactivities/{bio_id}/foods")
    bfood = ok200(f"/v1/bioactivities/{bio_id}/foods") or {}
    list_suite(f"/v1/bioactivities/{bio_id}/foods")
    check(
        "/v1/bioactivities/{bioactivity_id}/foods",
        "total = detail.n_foods",
        bfood.get("page", {}).get("total") == bio.get("n_foods"),
        f"{bfood.get('page', {}).get('total')} vs {bio.get('n_foods')}",
    )
    detail_404("/v1/bioactivities/{id}/foods")

    # 6. Triplets behind the garlic→chemical edge, and one attestation.
    auth_suite("/v1/triplets")
    trips = list_suite("/v1/triplets")
    filter_suite(
        "/v1/triplets",
        "head_id",
        food_id,
        "e999999999",
        lambda x: x.get("head_id") == food_id,
        closed=False,
    )
    filter_suite(
        "/v1/triplets",
        "tail_id",
        chem_id,
        "e999999999",
        lambda x: x.get("tail_id") == chem_id,
        closed=False,
    )
    for rel in (
        "contains",
        "is_a",
        "worsens",
        "reduces",
        "exhibits",
        "measured",
        "r1",
        "r5",
        "r6",
    ):
        filter_suite("/v1/triplets", "relationship", rel, None)
    filter_suite("/v1/triplets", "relationship", "contains", "bogus")
    src = next((t["source"] for t in trips if t.get("source")), "ptfi")
    filter_suite(
        "/v1/triplets",
        "source",
        src,
        "no-such-source",
        lambda x: x.get("source") == src,
        closed=False,
    )
    trip = next((t for t in trips if t.get("attestations")), trips[0] if trips else {})
    trip_id = trip.get("triplet_id", 1)
    auth_suite(f"/v1/triplets/{trip_id}")
    tdet = (ok200(f"/v1/triplets/{trip_id}") or {}).get("data", {})
    check(
        "/v1/triplets/{triplet_id}",
        "unknown id → 404",
        call("/v1/triplets/0").status_code == 404,
    )
    att_id = (tdet.get("attestations") or [{}])[0].get("attestation_id") or (
        trip.get("attestations") or [{}]
    )[0].get("attestation_id")
    auth_suite(f"/v1/attestations/{att_id or 'x'}")
    if att_id:
        ok200(f"/v1/attestations/{att_id}")
    else:
        check(
            "/v1/attestations/{attestation_id}",
            "200 + schema",
            False,
            "no attestation id found",
        )
    check(
        "/v1/attestations/{attestation_id}",
        "unknown id → 404",
        call("/v1/attestations/nope").status_code == 404,
    )

    # 7. Bundles + stats sanity.
    auth_suite("/v1/bundles")
    bundles = (ok200("/v1/bundles") or {}).get("data", [])
    check(
        "/v1/bundles",
        "≥1 bundle with a resolving download link",
        bool(bundles)
        and httpx.head(
            bundles[0].get("download_link", ""), follow_redirects=True, timeout=30
        ).status_code
        == 200,
    )
    check(
        "/v1/stats",
        "stats counts > 0",
        all(v > 0 for v in (stats.get("data") or {}).values() if isinstance(v, int)),
        json.dumps(stats.get("data"))[:120],
    )


# ---------------------------------------------------------------- coverage + report
def report() -> None:
    lines = [
        "# /v1 user-story run — " + time.strftime("%Y-%m-%d %H:%M"),
        "",
        f"Base: `{BASE}`",
        "",
    ]
    missing_ops = [op for op in OPS if op not in hits]
    missing_params = {
        op: sorted(
            {p["name"] for p in OPS[op].get("parameters", []) if p.get("in") == "query"}
            - hits[op]
        )
        for op in OPS
    }
    missing_params = {k: v for k, v in missing_params.items() if v}
    n_pass = sum(r["status"] == "PASS" for r in results)
    n_fail = len(results) - n_pass
    lines += [
        "## Coverage",
        f"- Operations in live OpenAPI: {len(OPS)}; exercised: {len(OPS) - len(missing_ops)}; missing: {missing_ops or 'none'}",
        f"- Query params never sent: {missing_params or 'none'}",
        f"- Requests made: {sum(1 for _ in open(OUT / 'requests.log'))}",
        f"- Checks: {n_pass} PASS / {n_fail} FAIL",
        "",
        "## Failures",
        "",
    ]
    for r in results:
        if r["status"] == "FAIL":
            lines.append(f"- `{r['op']}` — {r['check']} — {r['detail']}")
    lines += [
        "",
        "## Matrix",
        "",
        "| Operation | PASS | FAIL | failing checks |",
        "|---|---|---|---|",
    ]
    for op in sorted(OPS) + sorted({r["op"] for r in results} - set(OPS)):
        rs = [r for r in results if r["op"] == op]
        fails = [r["check"] for r in rs if r["status"] == "FAIL"]
        lines.append(
            f"| `{op}` | {len(rs) - len(fails)} | {len(fails)} | {'; '.join(fails)} |"
        )
    (OUT / "results.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[: 12 + n_fail]))
    print(f"\nfull report: {OUT / 'results.md'}")
    sys.exit(1 if n_fail or missing_ops or missing_params else 0)


if __name__ == "__main__":
    try:
        story()
    except Exception:
        import traceback

        traceback.print_exc()
    finally:
        LOG.flush()
        report()
