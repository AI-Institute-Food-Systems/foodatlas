"""Tests for the EntityLUT (placeholder-free disambiguation)."""

from src.pipeline.entities.utils.lut import EntityLUT


def test_add_and_lookup() -> None:
    lut = EntityLUT()
    lut.add("food", "apple", "e1")
    assert lut.lookup("food", "apple") == ["e1"]


def test_lookup_case_insensitive() -> None:
    lut = EntityLUT()
    lut.add("food", "Apple", "e1")
    assert lut.lookup("food", "apple") == ["e1"]
    assert lut.lookup("food", "APPLE") == ["e1"]


def test_lookup_missing() -> None:
    lut = EntityLUT()
    assert lut.lookup("food", "banana") == []


def test_lookup_unique() -> None:
    lut = EntityLUT()
    lut.add("chemical", "water", "e10")
    assert lut.lookup_unique("chemical", "water") == "e10"


def test_lookup_unique_ambiguous() -> None:
    lut = EntityLUT()
    lut.add("chemical", "acid", "e10")
    lut.add("chemical", "acid", "e20")
    assert lut.lookup_unique("chemical", "acid") is None


def test_ambiguous_entries() -> None:
    lut = EntityLUT()
    lut.add("chemical", "acid", "e10")
    lut.add("chemical", "acid", "e20")
    lut.add("chemical", "water", "e30")
    ambig = lut.ambiguous_entries("chemical")
    assert "acid" in ambig
    assert "water" not in ambig


def test_contains() -> None:
    lut = EntityLUT()
    lut.add("food", "apple", "e1")
    assert lut.contains("food", "apple")
    assert not lut.contains("food", "banana")


def test_no_duplicate_ids() -> None:
    lut = EntityLUT()
    lut.add("food", "apple", "e1")
    lut.add("food", "apple", "e1")
    assert lut.lookup("food", "apple") == ["e1"]


def test_get_food_lut() -> None:
    lut = EntityLUT()
    lut.add("food", "apple", "e1")
    lut.add("food", "banana", "e2")
    result = lut.get_food_lut()
    assert result == {"apple": ["e1"], "banana": ["e2"]}


def test_get_chemical_lut() -> None:
    lut = EntityLUT()
    lut.add("chemical", "water", "e10")
    result = lut.get_chemical_lut()
    assert result == {"water": ["e10"]}


def test_multiple_entity_types() -> None:
    lut = EntityLUT()
    lut.add("food", "sugar", "e1")
    lut.add("chemical", "sugar", "e2")
    assert lut.lookup("food", "sugar") == ["e1"]
    assert lut.lookup("chemical", "sugar") == ["e2"]
