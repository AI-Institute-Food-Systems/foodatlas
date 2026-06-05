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


class TestNormalizeUnit:
    """Unit normalization: superscript-1 and no-space 'per' notation."""

    # ------------------------------------------------------------------
    # parse_conc: unit string returned after normalization
    # ------------------------------------------------------------------

    def test_parse_superscript_mgkg(self) -> None:
        assert parse_conc("5.0 mgkg\u22121") == ("5.0", "mg/kg")

    def test_parse_superscript_mgg(self) -> None:
        assert parse_conc("5.0 mgg\u22121") == ("5.0", "mg/g")

    def test_parse_superscript_mg100g(self) -> None:
        assert parse_conc("2.5 mg100g\u22121") == ("2.5", "mg/100g")

    def test_parse_superscript_ugg(self) -> None:
        assert parse_conc("1.0 µgg\u22121") == ("1.0", "µg/g")

    def test_parse_superscript_gkg(self) -> None:
        assert parse_conc("10.0 gkg\u22121") == ("10.0", "g/kg")

    def test_parse_superscript_mgL(self) -> None:
        assert parse_conc("50.0 mgL\u22121") == ("50.0", "mg/l")

    def test_parse_dot_notation(self) -> None:
        assert parse_conc("3.0 mg·100g\u22121") == ("3.0", "mg/100g")

    def test_parse_dot_notation_mgg(self) -> None:
        assert parse_conc("1.5 mg·g\u22121") == ("1.5", "mg/g")

    def test_parse_superscript_en_dash(self) -> None:
        # U+2013 en-dash variant
        assert parse_conc("2.0 mgkg\u20131") == ("2.0", "mg/kg")

    def test_parse_superscript_with_fw(self) -> None:
        assert parse_conc("1.0 mgg\u22121 fw") == ("1.0", "mg/g fw")

    def test_parse_superscript_with_dw(self) -> None:
        assert parse_conc("2.0 mg100g\u22121 dw") == ("2.0", "mg/100g dw")

    def test_parse_nospace_mgper100g(self) -> None:
        assert parse_conc("5.0 mgper100g") == ("5.0", "mg/100g")

    def test_parse_nospace_gper100g(self) -> None:
        assert parse_conc("1.0 gper100g") == ("1.0", "g/100g")

    def test_parse_nospace_mgperg(self) -> None:
        assert parse_conc("2.0 mgperg") == ("2.0", "mg/g")

    # ------------------------------------------------------------------
    # convert_conc: end-to-end correctness after normalization
    # ------------------------------------------------------------------

    def test_convert_mgkg_superscript(self) -> None:
        val, unit = convert_conc("910", "mgkg\u22121")
        assert unit == "mg/100g"
        assert abs(val - 91.0) < 0.01

    def test_convert_mgg_superscript(self) -> None:
        val, unit = convert_conc("1.5", "mgg\u22121")
        assert unit == "mg/100g"
        assert abs(val - 150.0) < 0.01

    def test_convert_mg100g_superscript(self) -> None:
        val, unit = convert_conc("12.5", "mg100g\u22121")
        assert unit == "mg/100g"
        assert abs(val - 12.5) < 0.01

    def test_convert_ugg_superscript(self) -> None:
        val, unit = convert_conc("60", "µgg\u22121")
        assert unit == "mg/100g"
        assert abs(val - 6.0) < 0.01

    def test_convert_gkg_superscript(self) -> None:
        val, unit = convert_conc("1.0", "gkg\u22121")
        assert unit == "mg/100g"
        assert abs(val - 100.0) < 0.01

    def test_convert_dot_mg100g(self) -> None:
        val, unit = convert_conc("5.0", "mg·100g\u22121")
        assert unit == "mg/100g"
        assert abs(val - 5.0) < 0.01

    def test_convert_dot_mgg(self) -> None:
        val, unit = convert_conc("1.5", "mg·g\u22121")
        assert unit == "mg/100g"
        assert abs(val - 150.0) < 0.01

    def test_convert_en_dash_variant(self) -> None:
        val, unit = convert_conc("910", "mgkg\u20131")
        assert unit == "mg/100g"
        assert abs(val - 91.0) < 0.01

    def test_convert_fw_with_superscript(self) -> None:
        val, unit = convert_conc("12.5", "mgg\u22121 fw")
        assert unit == "mg/100g"
        assert abs(val - 1250.0) < 0.01

    def test_convert_mgper100g(self) -> None:
        val, unit = convert_conc("5.0", "mgper100g")
        assert unit == "mg/100g"
        assert abs(val - 5.0) < 0.01

    def test_convert_gper100g(self) -> None:
        val, unit = convert_conc("1.0", "gper100g")
        assert unit == "mg/100g"
        assert abs(val - 1000.0) < 0.01

    def test_convert_mgperg(self) -> None:
        val, unit = convert_conc("2.0", "mgperg")
        assert unit == "mg/100g"
        assert abs(val - 200.0) < 0.01

    # ------------------------------------------------------------------
    # Regressions: existing units must not be affected by normalization
    # ------------------------------------------------------------------

    def test_regression_mg_per_g(self) -> None:
        val, unit = convert_conc("1.5", "mg/g")
        assert unit == "mg/100g"
        assert abs(val - 150.0) < 0.01

    def test_regression_mg_per_kg(self) -> None:
        val, unit = convert_conc("910", "mg/kg")
        assert unit == "mg/100g"
        assert abs(val - 91.0) < 0.01

    def test_regression_mg_per_100g(self) -> None:
        val, unit = convert_conc("12.5", "mg/100g")
        assert unit == "mg/100g"
        assert abs(val - 12.5) < 0.01

    def test_regression_ug_per_g(self) -> None:
        val, unit = convert_conc("60", "µg/g")
        assert unit == "mg/100g"
        assert abs(val - 6.0) < 0.01

    def test_regression_g_per_100g(self) -> None:
        val, unit = convert_conc("1.0", "g/100g")
        assert unit == "mg/100g"
        assert abs(val - 1000.0) < 0.01

    def test_regression_fw_suffix(self) -> None:
        val, unit = convert_conc("12.5", "mg/100g fw")
        assert unit == "mg/100g"
        assert abs(val - 12.5) < 0.01

    def test_regression_volume_denom(self) -> None:
        val, unit = convert_conc("50", "mg/ml")
        assert unit == "mg/100g"
        assert abs(val - 5000.0) < 0.01

    def test_regression_percent_still_rejected(self) -> None:
        assert convert_conc("5", "%") is None

    def test_regression_molar_still_rejected(self) -> None:
        assert convert_conc("1.5", "mmol/l") is None

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_superscript_unknown_unit_pair_unchanged(self) -> None:
        # 'xyzabc\u22121' — not a known unit pair, should not convert
        assert convert_conc("1.0", "xyzabc\u22121") is None

    def test_per_not_replaced_in_percent(self) -> None:
        # 'percent' — no letter precedes 'per', so no substitution.
        # parse_conc returns the unit as-is; convert_conc rejects it.
        assert parse_conc("5percent") == ("5", "percent")
        assert convert_conc("5", "percent") is None

    def test_ngg_superscript(self) -> None:
        val, unit = convert_conc("0.5", "ngg\u22121")
        assert unit == "mg/100g"
        assert abs(val - 5e-5) < 1e-7
