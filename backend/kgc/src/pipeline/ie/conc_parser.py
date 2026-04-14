"""Parse and convert raw IE concentration strings.

Parsing splits a raw string into ``(value, unit)``.
Conversion normalises the parsed unit to ``mg/100g`` where possible.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------

# Mass units → grams.
_MASS: dict[str, float] = {
    "kg": 1e3,
    "g": 1,
    "grams": 1,
    "mg": 1e-3,
    "µg": 1e-6,
    "ug": 1e-6,
    "μg": 1e-6,
    "ng": 1e-9,
}

# Volume units → litres.
_VOLUME: dict[str, float] = {
    "l": 1,
    "ml": 1e-3,
    "µl": 1e-6,
    "ul": 1e-6,
    "μl": 1e-6,
}

# Maximum plausible mg/100g (sanity check).
_MAX_MG_100G = 1e5

# Regex to split a denominator like "100g" into ("100", "g").
_RE_DEN_FACTOR = re.compile(r"^(\d+)?(.+)$")

# All dashes normalised to ASCII hyphen for range splitting.
_RANGE_CHARS = re.compile("[\u2010-\u2015\u2212~\u223c]")


def _resolve_numeric(value_str: str) -> float | None:
    """Parse a value string that may contain a range or ± into a float."""
    # Strip leading approximate markers.
    s = value_str.lstrip("<>\u2248~\u223c")

    # Strip ± deviation (take the central value).
    if "±" in s or "\u00b1" in s:
        s = re.split(r"[±\u00b1]", s)[0]

    # Normalise range separators to ASCII hyphen.
    s = _RANGE_CHARS.sub("-", s)

    # Handle ranges → midpoint.
    if "-" in s:
        parts = s.split("-", 1)
        try:
            a, b = float(parts[0]), float(parts[1])
        except ValueError:
            return None
        return (a + b) / 2

    try:
        return float(s)
    except ValueError:
        return None


def _convert_unit(unit_str: str) -> float | None:
    """Return the multiplier to convert ``(value, unit)`` to mg/100g.

    Returns ``None`` for unconvertible units (molar, energy, etc.).
    """
    # Strip weight-type suffix (fw/dw) — already informational only.
    clean = unit_str.strip()
    for suffix, _ in _WEIGHT_TYPES:
        if clean.lower().endswith(suffix):
            clean = clean[: -len(suffix)].strip()
            break

    if clean == "%":
        return None

    parts = clean.split("/")
    if len(parts) != 2:
        return None

    return _convert_ratio(parts[0].lower(), parts[1].lower())


def _convert_ratio(num_unit: str, den_raw: str) -> float | None:
    """Convert a mass/weight or mass/volume ratio to mg/100g multiplier."""
    if num_unit not in _MASS:
        return None

    m = _RE_DEN_FACTOR.match(den_raw)
    if not m:
        return None
    den_factor_str, den_unit = m.group(1), m.group(2)
    den_factor = float(den_factor_str) if den_factor_str else 1.0

    grams_per_num = _MASS[num_unit]

    if den_unit in _MASS:
        grams_per_den = _MASS[den_unit]
        return (grams_per_num / grams_per_den) * (100 / den_factor) * 1e3
    if den_unit in _VOLUME:
        # Treat volume as weight (1ml ~ 1g for aqueous solutions).
        grams_per_den = _VOLUME[den_unit] * 1e3
        return (grams_per_num / grams_per_den) * (100 / den_factor) * 1e3

    return None


def convert_conc(value_str: str, unit_str: str) -> tuple[float, str] | None:
    """Convert a parsed ``(value, unit)`` to ``(mg/100g_value, "mg/100g")``.

    Returns ``None`` if the value or unit cannot be converted.

    Examples::

        >>> convert_conc("1.5", "mg/g")
        (150.0, 'mg/100g')
        >>> convert_conc("20\\u201350", "mg/100g")
        (35.0, 'mg/100g')
        >>> convert_conc("910", "mg/kg")
        (91.0, 'mg/100g')
        >>> convert_conc("45", "%")
        (450000.0, 'mg/100g')
    """
    if not value_str or not unit_str:
        return None

    numeric = _resolve_numeric(value_str)
    if numeric is None or numeric <= 0:
        return None

    multiplier = _convert_unit(unit_str)
    if multiplier is None:
        return None

    result = numeric * multiplier
    if result > _MAX_MG_100G:
        return None

    return (result, "mg/100g")
