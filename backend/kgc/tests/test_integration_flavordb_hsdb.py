"""Tests for HSDB loader module."""

import json

import pytest
from src.integration.flavordb.hsdb_loader import load_hsdb


class TestLoadHsdb:
    def test_loads_odor_and_taste(self, tmp_path):
        odor_data = {
            "Annotations": {
                "Annotation": [
                    {
                        "LinkedRecords": {"CID": [100]},
                        "Data": [
                            {"Value": {"StringWithMarkup": [{"String": "fruity"}]}}
                        ],
                        "URL": "https://example.com/odor",
                    }
                ]
            }
        }
        taste_data = {
            "Annotations": {
                "Annotation": [
                    {
                        "LinkedRecords": {"CID": [200]},
                        "Data": [
                            {"Value": {"StringWithMarkup": [{"String": "sweet"}]}}
                        ],
                        "URL": "https://example.com/taste",
                    }
                ]
            }
        }

        odor_path = tmp_path / "HSDB_Odor.json"
        taste_path = tmp_path / "HSDB_Taste.json"
        odor_path.write_text(json.dumps(odor_data))
        taste_path.write_text(json.dumps(taste_data))

        cid2odor, cid2taste = load_hsdb(tmp_path)
        assert 100 in cid2odor
        assert cid2odor[100][0]["value"] == "fruity"
        assert 200 in cid2taste
        assert cid2taste[200][0]["value"] == "sweet"

    def test_skips_without_linked_records(self, tmp_path):
        data = {
            "Annotations": {
                "Annotation": [
                    {
                        "Data": [
                            {"Value": {"StringWithMarkup": [{"String": "musty"}]}}
                        ],
                        "URL": "https://example.com",
                    }
                ]
            }
        }
        (tmp_path / "HSDB_Odor.json").write_text(json.dumps(data))
        (tmp_path / "HSDB_Taste.json").write_text(
            json.dumps({"Annotations": {"Annotation": []}})
        )

        cid2odor, cid2taste = load_hsdb(tmp_path)
        assert cid2odor == {}
        assert cid2taste == {}

    def test_raises_on_multi_markup(self, tmp_path):
        data = {
            "Annotations": {
                "Annotation": [
                    {
                        "LinkedRecords": {"CID": [100]},
                        "Data": [
                            {
                                "Value": {
                                    "StringWithMarkup": [
                                        {"String": "a"},
                                        {"String": "b"},
                                    ]
                                }
                            }
                        ],
                        "URL": "https://example.com",
                    }
                ]
            }
        }
        (tmp_path / "HSDB_Odor.json").write_text(json.dumps(data))
        (tmp_path / "HSDB_Taste.json").write_text(
            json.dumps({"Annotations": {"Annotation": []}})
        )

        with pytest.raises(ValueError, match="Expected 1"):
            load_hsdb(tmp_path)
