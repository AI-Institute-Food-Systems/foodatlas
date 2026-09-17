"""Unit tests for the shared table-search ILIKE helper."""

from src.repositories._search_util import build_ilike_pattern


class TestBuildIlikePattern:
    def test_wraps_in_percent(self) -> None:
        assert build_ilike_pattern("tomato") == "%tomato%"

    def test_strips_whitespace(self) -> None:
        assert build_ilike_pattern("  tomato  ") == "%tomato%"

    def test_returns_none_on_empty(self) -> None:
        assert build_ilike_pattern("") is None

    def test_returns_none_on_none(self) -> None:
        assert build_ilike_pattern(None) is None

    def test_returns_none_on_whitespace_only(self) -> None:
        """`if search:` on ' ' used to be truthy — dropping into an
        `ILIKE '% %'` clause that matched every row containing a space.
        Now we return None so callers can skip the WHERE clause."""
        assert build_ilike_pattern("   ") is None
        assert build_ilike_pattern("\t\n") is None

    def test_escapes_percent(self) -> None:
        """A literal `%` in the term must not act as a wildcard —
        otherwise searching for `50%` would return anything containing
        `50`."""
        assert build_ilike_pattern("50%") == r"%50\%%"

    def test_escapes_underscore(self) -> None:
        """`_` is a single-char wildcard in ILIKE."""
        assert build_ilike_pattern("a_b") == r"%a\_b%"

    def test_escapes_backslash(self) -> None:
        """`\\` is the escape character; escape it first so downstream
        `\\%` / `\\_` still parse as escaped metacharacters."""
        assert build_ilike_pattern("a\\b") == r"%a\\b%"

    def test_preserves_case(self) -> None:
        """Case-folding is ILIKE's job, not ours — preserve the
        caller's casing so exact-case searches remain possible if we
        ever swap ILIKE for LIKE somewhere."""
        assert build_ilike_pattern("Tomato") == "%Tomato%"
