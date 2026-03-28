"""Tests for concentration string parsing."""

import pytest
from src.preprocessing.conc_parser import (
    RANGES,
    _separate_conc_value_and_unit,
    parse_conc_string,
)


class TestSeparateConcValueAndUnit:
    """Test the value/unit splitting helper."""

    def test_simple(self) -> None:
        assert _separate_conc_value_and_unit("5.2mg") == (5.2, "mg")

    def test_integer(self) -> None:
        assert _separate_conc_value_and_unit("100g") == (100.0, "g")

    def test_with_uncertainty(self) -> None:
        val, unit = _separate_conc_value_and_unit("5.2±0.3mg")
        assert val == 5.2
        assert unit == "mg"

    def test_unit_only_digits(self) -> None:
        val, unit = _separate_conc_value_and_unit("50%")
        assert val == 50.0
        assert unit == "%"


class TestParseConcString:
    """Test concentration string parsing."""

    def test_none_input(self) -> None:
        assert parse_conc_string(None) == (None, None, None)

    def test_nan_input(self) -> None:
        assert parse_conc_string(float("nan")) == (None, None, None)

    def test_simple_mg(self) -> None:
        val, unit, wt = parse_conc_string("5mg")
        assert val == 5.0
        assert unit == "mg"
        assert wt is None

    def test_decimal(self) -> None:
        val, unit, _wt = parse_conc_string("3.14mg/100g")
        assert val == pytest.approx(3.14)
        assert unit == "mg/100g"

    def test_range_mean(self) -> None:
        val, unit, _wt = parse_conc_string("2-4mg")
        assert val == pytest.approx(3.0)
        assert unit == "mg"

    def test_unicode_range(self) -> None:
        val, unit, _wt = parse_conc_string("2\u20134mg")
        assert val == pytest.approx(3.0)
        assert unit == "mg"

    def test_fresh_weight(self) -> None:
        _val, _unit, wt = parse_conc_string("5mg/100gfw")
        assert wt == "fresh"

    def test_dry_weight(self) -> None:
        _val, _unit, wt = parse_conc_string("5mg/100gdw")
        assert wt == "dry"

    def test_freshweight_long(self) -> None:
        _, _, wt = parse_conc_string("5mg/100gfreshweight")
        assert wt == "fresh"

    def test_dryweight_long(self) -> None:
        _, _, wt = parse_conc_string("5mg/100gdryweight")
        assert wt == "dry"

    def test_uncertainty_stripped(self) -> None:
        val, unit, _ = parse_conc_string("5.2±0.3mg")
        assert val == pytest.approx(5.2)
        assert unit == "mg"

    def test_invalid_string(self) -> None:
        assert parse_conc_string("not a conc") == (None, None, None)

    def test_empty_string(self) -> None:
        assert parse_conc_string("") == (None, None, None)

    def test_whitespace_stripped(self) -> None:
        val, unit, _ = parse_conc_string("5 mg")
        assert val == 5.0
        assert unit == "mg"

    def test_interpunct_removed(self) -> None:
        val, unit, _ = parse_conc_string("5·mg")
        assert val == 5.0
        assert unit == "mg"

    def test_percentage(self) -> None:
        val, _unit, _ = parse_conc_string("5%")
        assert val == 5.0

    def test_range_different_units_returns_none(self) -> None:
        assert parse_conc_string("5mg-10g") == (None, None, None)


class TestRangesConstant:
    """Test the RANGES constant is properly defined."""

    def test_contains_standard_hyphen(self) -> None:
        assert "\u002d" in RANGES

    def test_contains_unicode_dashes(self) -> None:
        assert "\u2013" in RANGES

    def test_contains_to(self) -> None:
        assert "to" in RANGES
