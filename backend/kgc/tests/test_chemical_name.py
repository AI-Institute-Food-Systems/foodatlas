"""Tests for chemical name standardization."""

import pytest
from src.preprocessing.chemical_name import standardize_chemical_name


class TestGreekLetterNormalization:
    """Test Greek letter replacement."""

    def test_alpha_replacement(self) -> None:
        assert "alpha" in standardize_chemical_name("\u03b1-carotene")

    def test_beta_replacement(self) -> None:
        assert "beta" in standardize_chemical_name("\u03b2-carotene")

    def test_gamma_replacement(self) -> None:
        assert "gamma" in standardize_chemical_name("\u03b3-tocopherol")

    def test_multiple_greek_letters(self) -> None:
        result = standardize_chemical_name("\u03b1-\u03b2-compound")
        assert "alpha" in result
        assert "beta" in result

    def test_no_greek_letters(self) -> None:
        assert standardize_chemical_name("caffeine") == "caffeine"


class TestPunctuationStandardization:
    """Test punctuation normalization."""

    def test_unicode_hyphen(self) -> None:
        result = standardize_chemical_name("alpha\u2010carotene")
        assert "-" in result

    def test_em_dash(self) -> None:
        result = standardize_chemical_name("alpha\u2014carotene")
        assert "-" in result

    def test_unicode_quotes(self) -> None:
        result = standardize_chemical_name("\u201ctest\u201d")
        assert '"' in result

    def test_empty_string(self) -> None:
        assert standardize_chemical_name("") == ""

    def test_plain_ascii(self) -> None:
        name = "alpha-tocopherol"
        assert standardize_chemical_name(name) == name


class TestCombined:
    """Test combined Greek letter + punctuation normalization."""

    @pytest.mark.parametrize(
        ("raw", "expected_substr"),
        [
            ("\u03b1-carotene", "alpha"),
            ("\u03b2\u2010sitosterol", "beta"),
            ("\u03b4-tocopherol", "delta"),
        ],
    )
    def test_combined(self, raw: str, expected_substr: str) -> None:
        result = standardize_chemical_name(raw)
        assert expected_substr in result
