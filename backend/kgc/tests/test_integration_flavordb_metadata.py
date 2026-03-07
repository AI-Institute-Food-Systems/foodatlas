"""Tests for FlavorDB metadata module."""

import pandas as pd
from src.integration.flavordb.flavor_metadata import (
    extract_flavordb_metadata,
    extract_hsdb_metadata,
    fuzzy_match_flavors,
)


class TestExtractFlavordbMetadata:
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
        entity_pc_ids = {100}
        pc_id_to_name = {100: "ethanol"}

        result = extract_flavordb_metadata(data, entity_pc_ids, pc_id_to_name)
        flavors = set(result["_flavor"])
        assert "fruity" in flavors
        assert "sweet" in flavors
        assert "sour" in flavors

    def test_skips_non_entity_cids(self):
        data = {
            "999": {
                "flavor_profile": "fruity",
                "taste": "",
                "odor": "",
                "fooddb_flavor_profile": "",
                "super_sweet": "",
                "bitter": False,
            }
        }
        result = extract_flavordb_metadata(data, set(), {})
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
        result = extract_flavordb_metadata(data, {100}, {100: "test"})
        assert "bitter" in set(result["_flavor"])


class TestExtractHsdbMetadata:
    def test_extracts_entries(self):
        cid2odor = {100: [{"value": "musty", "hsdb_url": "http://a"}]}
        cid2taste = {200: [{"value": "sweet", "hsdb_url": "http://b"}]}
        pc_to_name = {100: "chem_a", 200: "chem_b"}

        result = extract_hsdb_metadata(cid2odor, cid2taste, set(), pc_to_name)
        assert len(result) == 2

    def test_skips_existing_cids(self):
        cid2odor = {100: [{"value": "musty", "hsdb_url": "http://a"}]}
        result = extract_hsdb_metadata(cid2odor, {}, {100}, {100: "chem_a"})
        assert result.empty

    def test_skips_unmapped_cids(self):
        cid2odor = {999: [{"value": "musty", "hsdb_url": "http://a"}]}
        result = extract_hsdb_metadata(cid2odor, {}, set(), {})
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
        result = fuzzy_match_flavors(pubchem, ref, threshold=90)
        assert len(result) == 1
        assert result.iloc[0]["_flavor"] == "sweet"

    def test_empty_input(self):
        empty = pd.DataFrame({"_flavor": []})
        ref = pd.Series(["sweet"])
        result = fuzzy_match_flavors(empty, ref)
        assert result.empty

    def test_replaces_with_matched(self):
        pubchem = pd.DataFrame(
            {
                "_flavor": ["swet"],
                "_pubchem_id": [100],
            }
        )
        ref = pd.Series(["sweet", "sour"])
        result = fuzzy_match_flavors(pubchem, ref, threshold=70)
        assert len(result) == 1
        assert result.iloc[0]["_flavor"] == "sweet"
