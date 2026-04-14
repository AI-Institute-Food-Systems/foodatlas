"""Tests for conc_parser — splitting and converting raw concentration strings."""

import pytest
from src.pipeline.ie.conc_parser import convert_conc, parse_conc


class TestParseConc:
    def test_empty_string(self) -> None:
        assert parse_conc("") == ("", "")

    def test_whitespace_only(self) -> None:
        assert parse_conc("   ") == ("", "")

    def test_simple_value_unit(self) -> None:
        assert parse_conc("1.5mg/g") == ("1.5", "mg/g")

    def test_value_unit_with_space(self) -> None:
        assert parse_conc("1.5 mg/g") == ("1.5", "mg/g")

    def test_percentage(self) -> None:
        assert parse_conc("5%") == ("5", "%")

    def test_unicode_dash_range(self) -> None:
        assert parse_conc("20\u201350mg") == ("20\u201350", "mg")

    def test_range_with_spaces(self) -> None:
        assert parse_conc("20 - 50 mg") == ("20-50", "mg")

    def test_to_range(self) -> None:
        assert parse_conc("20 to 50 mg") == ("20-50", "mg")

    def test_plus_minus(self) -> None:
        assert parse_conc("0.3\u00b10.1\u00b5g/mL") == ("0.3\u00b10.1", "\u00b5g/mL")

    def test_approx_less_than(self) -> None:
        assert parse_conc("<0.01mg/g") == ("<0.01", "mg/g")

    def test_approx_greater_than(self) -> None:
        assert parse_conc(">5mg/g") == (">5", "mg/g")

    def test_weight_type_fw(self) -> None:
        assert parse_conc("12.5mg/100gfw") == ("12.5", "mg/100g fw")

    def test_weight_type_dw(self) -> None:
        assert parse_conc("3.2mg/gdw") == ("3.2", "mg/g dw")

    def test_weight_type_freshweight(self) -> None:
        assert parse_conc("1.0mg/gfreshweight") == ("1.0", "mg/g fw")

    def test_compound_unit(self) -> None:
        assert parse_conc("50µg/100ml") == ("50", "µg/100ml")

    def test_unparseable_text(self) -> None:
        assert parse_conc("trace") is None

    def test_unparseable_no_unit(self) -> None:
        assert parse_conc("42") is None

    def test_unparseable_no_alpha_unit(self) -> None:
        assert parse_conc("5/100") is None

    @pytest.mark.parametrize(
        "raw",
        ["present", "detected", "n/a", "abundant"],
    )
    def test_qualitative_strings_unparseable(self, raw: str) -> None:
        assert parse_conc(raw) is None


class TestConvertConc:
    def test_mg_per_g(self) -> None:
        val, unit = convert_conc("1.5", "mg/g")
        assert unit == "mg/100g"
        assert abs(val - 150.0) < 0.01

    def test_mg_per_kg(self) -> None:
        val, unit = convert_conc("910", "mg/kg")
        assert unit == "mg/100g"
        assert abs(val - 91.0) < 0.01

    def test_mg_per_100g_passthrough(self) -> None:
        val, unit = convert_conc("315.1", "mg/100g")
        assert unit == "mg/100g"
        assert abs(val - 315.1) < 0.01

    def test_ug_per_g(self) -> None:
        val, unit = convert_conc("60", "µg/g")
        assert unit == "mg/100g"
        assert abs(val - 6.0) < 0.01

    def test_percentage_not_converted(self) -> None:
        assert convert_conc("5", "%") is None

    def test_range_midpoint(self) -> None:
        val, unit = convert_conc("20\u201350", "mg/100g")
        assert unit == "mg/100g"
        assert abs(val - 35.0) < 0.01

    def test_plus_minus_strips_deviation(self) -> None:
        val, unit = convert_conc("0.3\u00b10.1", "mg/g")
        assert unit == "mg/100g"
        assert abs(val - 30.0) < 0.01

    def test_approx_stripped(self) -> None:
        val, unit = convert_conc("<0.01", "mg/g")
        assert unit == "mg/100g"
        assert abs(val - 1.0) < 0.01

    def test_fw_suffix_ignored(self) -> None:
        val, unit = convert_conc("12.5", "mg/100g fw")
        assert unit == "mg/100g"
        assert abs(val - 12.5) < 0.01

    def test_volume_denominator(self) -> None:
        val, unit = convert_conc("50", "mg/ml")
        assert unit == "mg/100g"
        assert abs(val - 5000.0) < 0.01

    def test_empty_value_returns_none(self) -> None:
        assert convert_conc("", "mg/g") is None

    def test_empty_unit_returns_none(self) -> None:
        assert convert_conc("1.5", "") is None

    def test_molar_unit_returns_none(self) -> None:
        assert convert_conc("1.5", "mmol/l") is None

    def test_bare_mass_returns_none(self) -> None:
        assert convert_conc("1.5", "mg") is None

    def test_exceeds_max_returns_none(self) -> None:
        assert convert_conc("999999", "mg/g") is None
