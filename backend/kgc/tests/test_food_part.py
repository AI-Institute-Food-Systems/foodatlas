"""Tests for food part standardization."""

from src.preprocessing.food_part import standardize_food_part


class TestStandardizeFoodPart:
    """Test food part standardization."""

    def test_strip_whitespace(self) -> None:
        assert standardize_food_part("  leaf  ") == "leaf"

    def test_lowercase(self) -> None:
        assert standardize_food_part("SEED") == "seed"

    def test_combined(self) -> None:
        assert standardize_food_part("  Fruit Peel  ") == "fruit peel"

    def test_empty(self) -> None:
        assert standardize_food_part("") == ""

    def test_already_standard(self) -> None:
        assert standardize_food_part("root") == "root"
