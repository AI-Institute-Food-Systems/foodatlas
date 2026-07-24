"""Search-input normalization shared by table-side ILIKE filters.

Every table's search box (bioactivity, food composition, inferred
bioactivity, ...) builds an ILIKE substring pattern like ``f"%{term}%"``
around raw user input. That naive approach has two subtle bugs:

1. Whitespace-only input (e.g. an accidental space) is truthy in
   Python, so ``if search:`` fires and the query becomes
   ``ILIKE '% %'`` — matching every row whose name contains a space.
2. ``%`` and ``_`` in the user's term are ILIKE metacharacters.
   A search for ``50%`` becomes ``ILIKE '%50%%'`` which matches any row
   containing "50", not rows containing the literal "50%".

`build_ilike_pattern` returns None for blank input (caller skips the
clause entirely) and escapes the three ILIKE metacharacters
(``\\``, ``%``, ``_``) so a search for ``5_x`` matches literal ``5_x``
rather than every three-character sequence starting with 5 and ending
in x.
"""

from __future__ import annotations


def build_ilike_pattern(term: str | None) -> str | None:
    """Return a wildcard-wrapped, escape-safe ILIKE pattern.

    Returns ``None`` when ``term`` is empty or whitespace-only so the
    caller can skip appending the WHERE clause and its parameter
    entirely.
    """
    if term is None:
        return None
    cleaned = term.strip()
    if not cleaned:
        return None
    escaped = cleaned.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"
