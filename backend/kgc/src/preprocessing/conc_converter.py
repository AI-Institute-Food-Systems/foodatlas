"""Concentration unit conversion logic."""

from __future__ import annotations

from .constants import UNITS_ENERGY, UNITS_MASS, UNITS_MOLE, UNITS_VOLUME

KJ_PER_KCAL = 4.184
MAX_MG_PER_100G = 1e5


def _separate_unit_and_factor(conc_unit_str: str) -> tuple[str, str]:
    """Split a leading numeric factor from the unit string.

    E.g. "100g" → ("100", "g"), "mg" → ("", "mg").
    """
    i = 0
    while i < len(conc_unit_str) and conc_unit_str[i].isdigit():
        i += 1
    return conc_unit_str[:i], conc_unit_str[i:]


def _convert_numerator(value: float, unit: str) -> tuple[float, str]:
    """Convert a numerator unit to its standard form."""
    if unit == "%":
        return value, unit
    if unit in UNITS_MASS:
        return value * UNITS_MASS[unit] * 1000, "mg"
    if unit in UNITS_MOLE:
        return value * UNITS_MOLE[unit], "umol"
    if unit in UNITS_ENERGY:
        return value * UNITS_ENERGY[unit] / KJ_PER_KCAL, "kcal"
    return value, unit


def _convert_denominator(value: float, unit: str, factor: str) -> tuple[float, str]:
    """Convert a denominator unit and apply any numeric factor."""
    if factor:
        value = value / float(factor)

    if unit in UNITS_MASS:
        return value / UNITS_MASS[unit] * 100, "100g"
    if unit in UNITS_VOLUME:
        return value / UNITS_VOLUME[unit] / 1000 * 100, "100ml"
    if unit in UNITS_MOLE:
        return value / UNITS_MOLE[unit] * 1e6, "mol"
    return value, unit


def convert_conc_unit(
    conc_value: float | None,
    conc_unit: str | None,
) -> tuple[float | None, str | None]:
    """Convert a parsed concentration value+unit to standard units.

    Standard units: mg, mg/100g, umol, kcal, %.

    Args:
        conc_value: The numeric concentration value.
        conc_unit: The unit string (e.g. "mg/100g", "ug/g", "%").

    Returns:
        A tuple of (standardized_value, standardized_unit).
        Returns (None, None) if input is None or cannot be converted.

    """
    if conc_value is None or conc_unit is None:
        return None, None

    unit_terms = conc_unit.split("/")

    if len(unit_terms) == 1:
        factor, unit = _separate_unit_and_factor(unit_terms[0])
        if factor:
            msg = f"Invalid conc_unit: {conc_unit}"
            raise ValueError(msg)
        return _convert_numerator(conc_value, unit)

    if len(unit_terms) == 2:
        factor_num, unit_num = _separate_unit_and_factor(unit_terms[0])
        if factor_num:
            msg = f"Invalid conc_unit: {conc_unit}"
            raise ValueError(msg)

        factor_den, unit_den = _separate_unit_and_factor(unit_terms[1])

        value, unit_num = _convert_numerator(conc_value, unit_num)
        value, unit_den = _convert_denominator(value, unit_den, factor_den)

        return value, f"{unit_num}/{unit_den}"

    msg = f"Invalid conc_unit: {conc_unit}"
    raise ValueError(msg)


def check_conc_value(
    conc_value: float | None,
    conc_unit: str | None,
) -> tuple[float | None, str | None]:
    """Sanity-check a concentration value, rejecting obviously wrong ones.

    Values exceeding 1e5 mg/100g are treated as errors and nullified.
    """
    if conc_value is None or conc_unit is None:
        return conc_value, conc_unit

    if conc_unit in ("mg/100g", "mg/100g (converted)") and conc_value > MAX_MG_PER_100G:
        return None, None

    return conc_value, conc_unit
