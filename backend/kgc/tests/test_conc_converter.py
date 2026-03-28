"""Tests for concentration unit conversion."""

import pytest
from src.preprocessing.conc_converter import (
    check_conc_value,
    convert_conc_unit,
)


class TestConvertConcUnit:
    """Test unit conversion logic."""

    def test_none_value(self) -> None:
        assert convert_conc_unit(None, "mg") == (None, None)

    def test_none_unit(self) -> None:
        assert convert_conc_unit(5.0, None) == (None, None)

    def test_percent_passthrough(self) -> None:
        val, unit = convert_conc_unit(50.0, "%")
        assert val == 50.0
        assert unit == "%"

    def test_mg_passthrough(self) -> None:
        val, unit = convert_conc_unit(5.0, "mg")
        assert val == pytest.approx(5.0)
        assert unit == "mg"

    def test_g_to_mg(self) -> None:
        val, unit = convert_conc_unit(1.0, "g")
        assert val == pytest.approx(1000.0)
        assert unit == "mg"

    def test_ug_to_mg(self) -> None:
        val, unit = convert_conc_unit(1000.0, "ug")
        assert val == pytest.approx(1.0)
        assert unit == "mg"

    def test_kg_to_mg(self) -> None:
        val, unit = convert_conc_unit(1.0, "kg")
        assert val == pytest.approx(1e6)
        assert unit == "mg"

    def test_umol_passthrough(self) -> None:
        val, unit = convert_conc_unit(5.0, "umol")
        assert val == pytest.approx(5.0)
        assert unit == "umol"

    def test_mmol_to_umol(self) -> None:
        val, unit = convert_conc_unit(1.0, "mmol")
        assert val == pytest.approx(1000.0)
        assert unit == "umol"

    def test_kj_to_kcal(self) -> None:
        val, unit = convert_conc_unit(4.184, "kj")
        assert val == pytest.approx(1.0)
        assert unit == "kcal"

    def test_kcal_passthrough(self) -> None:
        val, unit = convert_conc_unit(100.0, "kcal")
        assert val == pytest.approx(100.0)
        assert unit == "kcal"


class TestConvertConcUnitFraction:
    """Test fractional unit conversion (e.g. mg/100g)."""

    def test_mg_per_100g(self) -> None:
        val, unit = convert_conc_unit(5.0, "mg/100g")
        assert val == pytest.approx(5.0)
        assert unit == "mg/100g"

    def test_ug_per_g(self) -> None:
        val, unit = convert_conc_unit(100.0, "ug/g")
        assert val == pytest.approx(10.0)
        assert unit == "mg/100g"

    def test_mg_per_ml(self) -> None:
        val, unit = convert_conc_unit(1.0, "mg/ml")
        assert val == pytest.approx(100.0)
        assert unit == "mg/100ml"

    def test_g_per_100g(self) -> None:
        val, unit = convert_conc_unit(1.0, "g/100g")
        assert val == pytest.approx(1000.0)
        assert unit == "mg/100g"

    def test_invalid_numerator_factor(self) -> None:
        with pytest.raises(ValueError, match="Invalid conc_unit"):
            convert_conc_unit(5.0, "100mg")

    def test_invalid_fraction_numerator_factor(self) -> None:
        with pytest.raises(ValueError, match="Invalid conc_unit"):
            convert_conc_unit(5.0, "100mg/g")

    def test_invalid_three_parts(self) -> None:
        with pytest.raises(ValueError, match="Invalid conc_unit"):
            convert_conc_unit(5.0, "mg/100g/ml")


class TestCheckConcValue:
    """Test concentration sanity checking."""

    def test_none_passthrough(self) -> None:
        assert check_conc_value(None, None) == (None, None)

    def test_valid_mg_100g(self) -> None:
        val, _unit = check_conc_value(500.0, "mg/100g")
        assert val == 500.0

    def test_excessive_mg_100g(self) -> None:
        assert check_conc_value(2e5, "mg/100g") == (None, None)

    def test_excessive_converted(self) -> None:
        assert check_conc_value(2e5, "mg/100g (converted)") == (None, None)

    def test_other_unit_not_checked(self) -> None:
        val, _unit = check_conc_value(2e5, "mg")
        assert val == 2e5

    def test_boundary_value(self) -> None:
        val, _unit = check_conc_value(1e5, "mg/100g")
        assert val == 1e5

    def test_just_over_boundary(self) -> None:
        assert check_conc_value(1e5 + 1, "mg/100g") == (None, None)
