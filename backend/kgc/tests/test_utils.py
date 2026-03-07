"""Tests for utility functions."""

import pytest
from src.utils.constants import get_lookup_key_by_id
from src.utils.merge_sets import merge_sets


class TestGetLookupKeyById:
    def test_foodon_id(self):
        key = get_lookup_key_by_id("foodon_id", "00001")
        assert key == "_FOODON_ID:00001"

    def test_pubchem_cid(self):
        key = get_lookup_key_by_id("pubchem_cid", 54670067)
        assert key == "_PubChem_Compound_ID:54670067"

    def test_fdc_nutrient_ids(self):
        key = get_lookup_key_by_id("fdc_nutrient_ids", "1003")
        assert key == "_FDC_Nutrient_ID:1003"

    def test_fdc_ids(self):
        key = get_lookup_key_by_id("fdc_ids", "12345")
        assert key == "_FDC_ID:12345"

    def test_unknown_id_type_raises(self):
        with pytest.raises(ValueError, match="Unknown ID type"):
            get_lookup_key_by_id("unknown_type", "123")


class TestMergeSets:
    def test_no_overlap(self):
        sets = [{"a", "b"}, {"c", "d"}, {"e", "f"}]
        result = merge_sets(sets)
        assert len(result) == 3

    def test_full_overlap(self):
        sets = [{"a", "b"}, {"b", "c"}, {"c", "d"}]
        result = merge_sets(sets)
        assert len(result) == 1
        assert result[0] == {"a", "b", "c", "d"}

    def test_partial_overlap(self):
        sets = [{"a", "b"}, {"b", "c"}, {"d", "e"}]
        result = merge_sets(sets)
        assert len(result) == 2
        merged = sorted(result, key=len, reverse=True)
        assert merged[0] == {"a", "b", "c"}
        assert merged[1] == {"d", "e"}

    def test_empty_input(self):
        result = merge_sets([])
        assert result == []

    def test_single_set(self):
        result = merge_sets([{"a", "b"}])
        assert len(result) == 1
        assert result[0] == {"a", "b"}
