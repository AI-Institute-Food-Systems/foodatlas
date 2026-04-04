"""Tests for conc_parser — splitting raw concentration strings."""

import pytest
from src.pipeline.ie.conc_parser import parse_conc


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
