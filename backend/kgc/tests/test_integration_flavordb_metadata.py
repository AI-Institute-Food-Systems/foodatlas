"""Tests for FlavorDB data cleaning module."""

import pandas as pd
from src.integration.data_cleaning.flavordb import (
    _extract_flavordb_rows,
    _extract_hsdb_rows,
    _fuzzy_match_flavors,
)


class TestExtractFlavordbRows:
    def test_extracts_descriptors(self):
        data = {
            "100": {
                "flavor_profile": "fruity@sweet",
                "taste": "sour",
                "odor": "",
                "fooddb_flavor_profile": "",
                "super_sweet": "",
                "bitter": False,
            }
        }

        result = _extract_flavordb_rows(data)
        flavors = set(result["_flavor"])
        assert "fruity" in flavors
        assert "sweet" in flavors
        assert "sour" in flavors
        assert (result["_source"] == "flavordb").all()
        assert result["_url"].str.contains("100").all()

    def test_empty_data_returns_empty(self):
        result = _extract_flavordb_rows({})
        assert result.empty

    def test_adds_bitter_when_true(self):
        data = {
            "100": {
                "flavor_profile": "",
                "taste": "",
                "odor": "",
                "fooddb_flavor_profile": "",
                "super_sweet": "",
                "bitter": True,
            }
        }
        result = _extract_flavordb_rows(data)
        assert "bitter" in set(result["_flavor"])


class TestExtractHsdbRows:
    def test_extracts_entries(self):
        cid2odor = {100: [{"value": "musty", "hsdb_url": "http://a"}]}
        cid2taste = {200: [{"value": "sweet", "hsdb_url": "http://b"}]}

        result = _extract_hsdb_rows(cid2odor, cid2taste, set())
        assert len(result) == 2
        assert (result["_source"] == "hsdb").all()

    def test_skips_existing_cids(self):
        cid2odor = {100: [{"value": "musty", "hsdb_url": "http://a"}]}
        result = _extract_hsdb_rows(cid2odor, {}, {100})
        assert result.empty


class TestFuzzyMatchFlavors:
    def test_filters_below_threshold(self):
        pubchem = pd.DataFrame(
            {
                "_flavor": ["sweet", "xyzzyx"],
                "_pubchem_id": [100, 200],
            }
        )
        ref = pd.Series(["sweet", "sour", "bitter"])
        result = _fuzzy_match_flavors(pubchem, ref, threshold=90)
        assert len(result) == 1
        assert result.iloc[0]["_flavor"] == "sweet"

    def test_empty_input(self):
        empty = pd.DataFrame({"_flavor": []})
        ref = pd.Series(["sweet"])
        result = _fuzzy_match_flavors(empty, ref)
        assert result.empty

    def test_replaces_with_matched(self):
        pubchem = pd.DataFrame(
            {
                "_flavor": ["swet"],
                "_pubchem_id": [100],
            }
        )
        ref = pd.Series(["sweet", "sour"])
        result = _fuzzy_match_flavors(pubchem, ref, threshold=70)
        assert len(result) == 1
        assert result.iloc[0]["_flavor"] == "sweet"
