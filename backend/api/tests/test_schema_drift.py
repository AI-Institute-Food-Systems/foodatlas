"""The test schema must not drift from the one backend/db actually builds.

`tests/dbharness.py` hand-writes `CREATE TABLE mv_food_chemical_composition
(…)` so the predicate tests have something to query. That makes it a
*second* definition of a schema whose real owner is
`backend/db/src/models/views.py` — two implementations of one thing with
nothing asserting they agree, which is the exact shape of the bug this
whole test effort exists to prevent. Without this file the harness could
keep passing against a fictional schema while production queries fail on
a renamed or retyped column, and it would look like stronger evidence
than it is.

It already drifted once: the harness declared `id TEXT` where the model
has `id: Mapped[int]`.

Source-text parsing rather than importing the models: backend/db is a
separate uv project with its own venv, and backend/api cannot import it.
Reading the file keeps the check honest without coupling the two
projects' dependency graphs.

Scope is deliberately narrow — every column the harness declares must
exist on the model with a compatible type. It does not require the
harness to declare every model column: the fixtures intentionally carry
only the columns the filter code reads, and demanding the full set would
make the fixture unreadable for no extra safety.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from tests.dbharness import DDL, MIRRORED_TABLES

# backend/api/tests -> backend/api -> backend -> <repo root>
_REPO_ROOT = Path(__file__).resolve().parents[3]
_VIEWS = _REPO_ROOT / "backend" / "db" / "src" / "models" / "views.py"

# SQLAlchemy column type -> the Postgres types that satisfy it. Loose on
# width (TEXT for String(20) is fine in a fixture) and strict on family:
# swapping INTEGER for TEXT is exactly the drift worth catching.
_COMPATIBLE: dict[str, frozenset[str]] = {
    "Text": frozenset({"TEXT", "VARCHAR"}),
    "String": frozenset({"TEXT", "VARCHAR"}),
    "Integer": frozenset({"INTEGER", "BIGINT", "SMALLINT"}),
    "Numeric": frozenset({"NUMERIC", "DECIMAL"}),
    "Float": frozenset({"REAL", "DOUBLE", "NUMERIC"}),
    "Boolean": frozenset({"BOOLEAN"}),
    "JSONB": frozenset({"JSONB"}),
    "ARRAY": frozenset({"TEXT[]"}),
    "DateTime": frozenset({"TIMESTAMP"}),
}


def _parse_ddl() -> dict[str, dict[str, str]]:
    """{table: {column: PG type}} from the harness DDL."""
    out: dict[str, dict[str, str]] = {}
    for ddl in DDL:
        m = re.search(r"CREATE TABLE (\w+)\s*\((.*)\)", ddl, re.S)
        assert m, f"unparseable DDL: {ddl[:60]}"
        table, body = m.group(1), m.group(2)
        cols: dict[str, str] = {}
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            # "chemical_classification TEXT[] ..." -> ("...", "TEXT[]")
            cols[parts[0]] = parts[1].upper()
        out[table] = cols
    return out


def _parse_models() -> dict[str, dict[str, str]]:
    """{table: {column: SQLAlchemy type}} from views.py."""
    src = _VIEWS.read_text(encoding="utf-8")
    out: dict[str, dict[str, str]] = {}
    # Split on class boundaries so a column can't be attributed to the
    # wrong table when several models sit in one file.
    chunks = re.split(r"\nclass\s+\w+\(", src)
    for chunk in chunks:
        tm = re.search(r'__tablename__\s*=\s*"([^"]+)"', chunk)
        if not tm:
            continue
        cols: dict[str, str] = {}
        for cm in re.finditer(
            r"^\s{4}(\w+):\s*Mapped\[.+?\]\s*=\s*mapped_column\(\s*([\w()\[\]]*)",
            chunk,
            re.M,
        ):
            name, raw = cm.group(1), cm.group(2)
            # `mapped_column(primary_key=True, ...)` has no explicit type;
            # SQLAlchemy infers it from the Mapped[...] annotation.
            if not raw or raw.startswith(("primary_key", "nullable", "server_default")):
                ann = re.search(rf"{name}:\s*Mapped\[(.+?)\]\s*=", chunk)
                raw = "Integer" if ann and "int" in ann.group(1) else "Text"
            cols[name] = raw.split("(")[0]
        out[tm.group(1)] = cols
    return out


def test_views_module_is_where_we_think_it_is() -> None:
    """Guards the guard — a moved file must not silently disable this."""
    assert _VIEWS.exists(), f"expected backend/db models at {_VIEWS}"


def test_parser_finds_the_mirrored_models() -> None:
    """A regex that matches nothing would make every check below vacuous."""
    models = _parse_models()
    for table in MIRRORED_TABLES:
        assert table in models, f"{table} not found in views.py"
        assert models[table], f"{table} parsed with zero columns"


@pytest.mark.parametrize("table", MIRRORED_TABLES)
def test_harness_columns_exist_on_the_model(table: str) -> None:
    ddl_cols = _parse_ddl()[table]
    model_cols = _parse_models()[table]
    unknown = sorted(set(ddl_cols) - set(model_cols))
    assert not unknown, (
        f"{table}: tests/dbharness.py declares columns that do not exist on "
        f"{_VIEWS.name}: {unknown}. Either the MV changed and the harness is "
        "now testing a fiction, or the harness has a typo — both make the "
        "predicate tests pass against a schema production does not have."
    )


@pytest.mark.parametrize("table", MIRRORED_TABLES)
def test_harness_column_types_are_compatible(table: str) -> None:
    ddl_cols = _parse_ddl()[table]
    model_cols = _parse_models()[table]
    mismatches: list[str] = []
    for col, pg_type in ddl_cols.items():
        sa_type = model_cols.get(col)
        if sa_type is None:
            continue  # reported by the test above
        allowed = _COMPATIBLE.get(sa_type)
        if allowed is None:
            mismatches.append(f"{col}: unmapped SQLAlchemy type {sa_type!r}")
        elif not any(pg_type.startswith(a) for a in allowed):
            mismatches.append(f"{col}: harness {pg_type} vs model {sa_type}")
    assert not mismatches, f"{table} type drift: {mismatches}"


# --- cross-language contracts -----------------------------------------

_FRONTEND = _REPO_ROOT / "frontend"
_FE_SOURCES = _FRONTEND / "components" / "entities" / "food" / "compositionSources.ts"
_FE_EVIDENCE = _FRONTEND / "types" / "Evidence.ts"


class TestFrontendSourceListMatchesBackend:
    """The two languages must agree on what the sources are.

    `COMPOSITION_SOURCES` (Python) and `SOURCE_OPTIONS` (TypeScript) are
    the same list written twice. When PTFI was added to one and not the
    other the symptoms were silent: `frontend/types/Evidence.ts` still
    declared `source_name: "FoodAtlas" | "FDC"`, so PTFI evidence was
    typed as impossible while the API happily returned it, and the modal's
    source picker could never offer it.

    Nothing catches that at build time — TypeScript only checks the
    frontend against itself, and pytest only checks the backend against
    itself. This is the seam between them.
    """

    @staticmethod
    def _fe_source_values() -> list[str]:
        src = _FE_SOURCES.read_text(encoding="utf-8")
        block = re.search(r"SOURCE_OPTIONS\s*=\s*\[(.*?)\]", src, re.S)
        assert block, "SOURCE_OPTIONS not found in compositionSources.ts"
        return re.findall(r'value:\s*"([^"]+)"', block.group(1))

    @staticmethod
    def _fe_source_labels() -> list[str]:
        src = _FE_SOURCES.read_text(encoding="utf-8")
        block = re.search(r"SOURCE_OPTIONS\s*=\s*\[(.*?)\]", src, re.S)
        assert block
        return re.findall(r'label:\s*"([^"]+)"', block.group(1))

    def test_frontend_files_exist(self) -> None:
        assert _FE_SOURCES.exists(), f"missing {_FE_SOURCES}"
        assert _FE_EVIDENCE.exists(), f"missing {_FE_EVIDENCE}"

    def test_source_keys_match(self) -> None:
        from src.repositories._sources import COMPOSITION_SOURCES  # noqa: PLC0415

        assert self._fe_source_values() == list(COMPOSITION_SOURCES), (
            "frontend SOURCE_OPTIONS and backend COMPOSITION_SOURCES disagree. "
            "A source present in only one is either unfilterable in the UI or "
            "sent to an API that will drop it."
        )

    def test_evidence_source_name_union_covers_every_source(self) -> None:
        """The union that silently excluded PTFI."""
        raw = _FE_EVIDENCE.read_text(encoding="utf-8")
        # Drop `//` lines first: the file keeps an older commented-out
        # FoodEvidence declaration above the live one, and matching that
        # corpse would assert against a type nothing uses.
        src = "\n".join(
            ln for ln in raw.splitlines() if not ln.lstrip().startswith("//")
        )
        # Scope to FoodEvidence: another type in this file declares a
        # loose `source_name: string`, and matching that one instead would
        # make this assertion silently unenforceable.
        block = re.search(r"export type FoodEvidence = \{(.*?)\n\};", src, re.S)
        assert block, "FoodEvidence not found in types/Evidence.ts"
        m = re.search(r"source_name:\s*([^;]+);", block.group(1))
        assert m, "FoodEvidence.reference.source_name not found"
        declared = set(re.findall(r'"([^"]+)"', m.group(1)))
        assert declared, (
            "FoodEvidence.reference.source_name is not a literal union "
            f"({m.group(1).strip()!r}); widening it to `string` removes the "
            "only compile-time guard on source names."
        )
        assert declared == set(self._fe_source_labels()), (
            f"types/Evidence.ts declares source_name as {sorted(declared)} but "
            f"the API can emit {sorted(self._fe_source_labels())}. Evidence "
            "from a source missing here is typed as impossible while arriving "
            "at runtime."
        )
