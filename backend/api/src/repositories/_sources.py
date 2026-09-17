"""The composition source filter, defined once.

Source filtering lives in two independent implementations: the rows path
builds a SQL WHERE clause, the counts path runs a Python loop over
already-fetched rows. They must agree — a row the SQL keeps and the loop
doesn't (or vice versa) shows up in the UI as a pager that promises rows
the table can't render.

They did not agree. `_build_query_parts` emitted the source predicate only
when *exactly one* source was selected, which was indistinguishable from
correct while there were two sources (deselecting one always left one).
PTFI made it a three-way choice and `{fdc, ptfi}` fell through to no WHERE
clause at all — the unfiltered set, 309 rows for pepper (raw) where the
answer is 106. Both paths are now derived from the definitions below, and
`test_composition_sources.py` executes them against a fixture rather than
asserting on a mock, so drift fails a test instead of a page.

DMD (Dairy Molecule Database) is retired from the public API surface
2026-07-06 — the DB column `dmd_evidences` stays populated on
`mv_food_chemical_composition` but is not selectable / filterable /
countable via this API, so it is deliberately absent here.

PTFI ships relative_abundance rather than mg/100g, so most of its rows
have no median_concentration. They are still real composition evidence and
are selectable / filterable / countable like any other source.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence

COMPOSITION_SOURCES: tuple[str, ...] = ("fdc", "foodatlas", "ptfi")


def evidence_column(source: str) -> str:
    """The evidence column backing a source key."""
    return source + "_evidences"


def parse_sources(filter_source: str) -> list[str]:
    """Allowlisted source keys from the '+'-joined filter param.

    Order follows COMPOSITION_SOURCES, not the caller's, so the generated
    SQL is stable for a given selection regardless of how the frontend
    happened to order the query string.
    """
    requested = {s for s in filter_source.split("+") if s}
    return [s for s in COMPOSITION_SOURCES if s in requested]


def source_sql(sources: Sequence[str]) -> str:
    """Parenthesised OR-group over the selected sources' evidence columns.

    e.g. ``(fdc_evidences IS NOT NULL OR ptfi_evidences IS NOT NULL)``.

    The parens are load-bearing. Callers AND this into a larger WHERE, and
    ``AND`` binds tighter than ``OR`` in SQL — a bare OR-group would parse
    as ``food_name = :name AND fdc_evidences IS NOT NULL OR ptfi_evidences
    IS NOT NULL`` and return every *other* food's PTFI rows too.
    """
    return "(" + " OR ".join(f"{evidence_column(s)} IS NOT NULL" for s in sources) + ")"


def any_source_sql() -> str:
    """OR-group over every exposed source — "has evidence we can render".

    Filters out DMD-only rows, which would otherwise render as blank rows
    in the composition table now that dmd_evidences is not selected.
    """
    return source_sql(COMPOSITION_SOURCES)


def row_has_source(row: Mapping[str, object], sources: Collection[str]) -> bool:
    """Python twin of :func:`source_sql`, for the counts path.

    ``.get`` rather than ``[]``: callers hand us rows from several queries
    and not all of them select every evidence column.
    """
    return any(row.get(evidence_column(s)) is not None for s in sources)
