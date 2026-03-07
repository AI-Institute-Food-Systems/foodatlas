"""Concentration string parsing — regex patterns and parse logic."""

from __future__ import annotations

import re

import numpy as np

from .constants import UNITS

APPROXIMATES = ["<", ">", "\u2248", "~", "\u223c"]

RANGES = [
    "\u002d",
    "\u2010",
    "\u2011",
    "\u2012",
    "\u2013",
    "\u2014",
    "\u2015",
    "\u2212",
    "~",
    "\u223c",
    "to",
]

WEIGHT_TYPES = [
    "freshweight",
    "offw",
    "fw",
    "dryweight",
    "dw",
    "dm",
]

FRESH_WEIGHT_TYPES = {"freshweight", "offw", "fw"}


def _get_regex() -> str:
    """Build the regex pattern for matching concentration strings."""
    re_value = r"(\d+(\.\d+)?(±\d+(\.\d+)?)?)"
    re_weight = f"({'|'.join(WEIGHT_TYPES)})"

    _re_unit = f"({'|'.join(UNITS)})"
    re_unit = rf"({_re_unit}(/(\d+)?{_re_unit})?)"

    return (
        rf"{re_value}"
        rf"({re_unit}?-{re_value})?"
        rf"{re_unit}{re_weight}?$"
    )


def _separate_conc_value_and_unit(conc_str: str) -> tuple[float, str]:
    """Split a concentration string into its numeric value and unit suffix."""
    i = 0
    while i < len(conc_str) and (conc_str[i].isdigit() or conc_str[i] in (".", "±")):
        i += 1

    conc_value = conc_str[:i]
    conc_unit = conc_str[i:]

    if "±" in conc_value:
        conc_value = conc_value.split("±")[0]

    return float(conc_value), conc_unit


def parse_conc_string(
    conc_raw: str | float | None,
) -> tuple[float | None, str | None, str | None]:
    """Parse a raw concentration string into value, unit, and weight type.

    Args:
        conc_raw: The raw concentration string (e.g. "5.2mg/100gfw").
            Accepts float to handle pandas NaN values.

    Returns:
        A tuple of (value, unit, weight_type) where weight_type is
        "fresh", "dry", or None. Returns (None, None, None) if the
        string cannot be parsed.

    """
    if conc_raw is None or not isinstance(conc_raw, str):
        return None, None, None

    conc_str = conc_raw.replace("·", "")
    for range_ in RANGES:
        conc_str = conc_str.replace(range_, "-")
    conc_str = "".join(conc_str.split())

    if not re.fullmatch(_get_regex(), conc_str):
        return None, None, None

    conc_weight: str | None = None
    for weight_type in WEIGHT_TYPES:
        if conc_str.endswith(weight_type):
            conc_str = conc_str[: -len(weight_type)]
            conc_weight = "fresh" if weight_type in FRESH_WEIGHT_TYPES else "dry"
            break

    if "-" in conc_str:
        conc_terms = conc_str.split("-")
        if len(conc_terms) != 2:
            return None, None, None

        val_first, unit_first = _separate_conc_value_and_unit(conc_terms[0])
        val_second, unit_second = _separate_conc_value_and_unit(conc_terms[1])

        if unit_first and unit_first != unit_second:
            return None, None, None

        conc_value: float = float(np.mean([val_first, val_second]))
        conc_unit: str = unit_second
    else:
        conc_value, conc_unit = _separate_conc_value_and_unit(conc_str)

    return conc_value, conc_unit, conc_weight
