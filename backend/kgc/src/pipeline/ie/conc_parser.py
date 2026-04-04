"""Parse raw IE concentration strings into (value, unit) pairs.

Ported from ``examples/preprocessing/_standardize_chemical_conc.py`` with a
lighter footprint: no pandas/numpy dependency, no unit conversion (that is a
downstream concern for ``conc_value`` / ``conc_unit``).
"""

from __future__ import annotations

import re

# Unicode dashes / range separators → kept as-is in the captured value.
_DASH = r"[\u002d\u2010\u2011\u2012\u2013\u2014\u2015\u2212~\u223c]"

# Approximate prefixes that may precede a numeric value.
_APPROX = r"[<>\u2248~\u223c]"

# A bare number: digits and dots, optionally with ± deviation.
_NUM = r"[\d.]+(?:\u00b1[\d.]+)?"

# Full pattern: optional approx, number, optional range, then unit.
_RE_CONC = re.compile(
    rf"^({_APPROX}?{_NUM}(?:{_DASH}{_NUM})?)"  # value (with optional range)
    rf"(.+)$",  # unit
)

# Weight-type suffixes (longest first to avoid partial matches).
_WEIGHT_TYPES: list[tuple[str, str]] = sorted(
    [
        ("freshweight", "fw"),
        ("offw", "fw"),
        ("fw", "fw"),
        ("dryweight", "dw"),
        ("dw", "dw"),
        ("dm", "dw"),
    ],
    key=lambda t: len(t[0]),
    reverse=True,
)


def parse_conc(raw: str) -> tuple[str, str] | None:
    """Split a raw concentration string into ``(value, unit)``.

    Returns ``("", "")`` for blank input.
    Returns ``None`` when the string cannot be parsed (no leading number,
    or no recognisable unit), which signals a diagnostic entry.

    Examples::

        >>> parse_conc("1.5mg/g")
        ('1.5', 'mg/g')
        >>> parse_conc("20\\u201350 mg")
        ('20\\u201350', 'mg')
        >>> parse_conc("")
        ('', '')
        >>> parse_conc("trace") is None
        True
    """
    if not raw:
        return ("", "")

    # Normalise "X to Y" ranges into "X-Y" before collapsing whitespace.
    s = re.sub(r"(\d)\s+to\s+(\d)", r"\1-\2", raw)
    s = "".join(s.split())  # collapse remaining whitespace

    if not s:
        return ("", "")

    # Strip weight-type suffix (case-insensitive).
    weight = ""
    s_lower = s.lower()
    for suffix, label in _WEIGHT_TYPES:
        if s_lower.endswith(suffix):
            s = s[: -len(suffix)]
            weight = label
            break

    m = _RE_CONC.match(s)
    if not m:
        return None

    value_str = m.group(1)
    unit_str = m.group(2)

    # Unit must contain at least one letter-like char or '%'.
    if not unit_str or not any(c.isalpha() or c == "%" for c in unit_str):
        return None

    if weight:
        unit_str = f"{unit_str} {weight}"

    return (value_str, unit_str)
