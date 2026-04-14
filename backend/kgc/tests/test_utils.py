"""Tests for utility functions."""

import pytest
from src.utils.constants import get_lookup_key_by_id


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
