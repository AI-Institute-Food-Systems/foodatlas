"""Tests for preprocessing constants."""

from src.preprocessing.constants import GREEK_LETTERS, PUNCTUATIONS
from src.preprocessing.constants.units import (
    UNITS,
    UNITS_ENERGY,
    UNITS_MASS,
    UNITS_MOLE,
    UNITS_VOLUME,
)


class TestGreekLetters:
    def test_has_expected_keys(self):
        expected = {
            "alpha",
            "beta",
            "gamma",
            "delta",
            "epsilon",
            "zeta",
            "eta",
            "theta",
            "omega",
            "kappa",
            "lambda",
            "iota",
            "sigma",
            "tao",
            "mu",
            "nu",
            "xi",
            "omicron",
            "pi",
            "rho",
            "upsilon",
            "phi",
            "chi",
            "psi",
        }
        assert set(GREEK_LETTERS.keys()) == expected

    def test_alpha_contains_standard_char(self):
        assert "α" in GREEK_LETTERS["alpha"]

    def test_beta_contains_standard_char(self):
        assert "β" in GREEK_LETTERS["beta"]


class TestPunctuations:
    def test_has_expected_keys(self):
        assert set(PUNCTUATIONS.keys()) == {"", "-", '"', "'", "->"}

    def test_hyphens_not_empty(self):
        assert len(PUNCTUATIONS["-"]) > 0


class TestUnits:
    def test_units_list_not_empty(self):
        assert len(UNITS) > 0

    def test_mass_conversions(self):
        assert UNITS_MASS["g"] == 1
        assert UNITS_MASS["kg"] == 1e3
        assert UNITS_MASS["mg"] == 1e-3

    def test_volume_conversions(self):
        assert UNITS_VOLUME["l"] == 1
        assert UNITS_VOLUME["ml"] == 1e-3

    def test_mole_conversions(self):
        assert UNITS_MOLE["mol"] == 1e6
        assert UNITS_MOLE["mmol"] == 1e3

    def test_energy_conversions(self):
        assert UNITS_ENERGY["kj"] == 1
        assert UNITS_ENERGY["kcal"] == 4.184
