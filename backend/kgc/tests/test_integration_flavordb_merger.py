"""Tests for FlavorDB merger module."""

import json
from unittest.mock import MagicMock

import pandas as pd
from src.integration.flavordb.merger import merge_flavordb


class TestMergeFlavordb:
    def _setup_data_dir(self, tmp_path):
        """Create minimal test data for FlavorDB merging."""
        flavordb_dir = tmp_path / "FlavorDB"
        flavordb_dir.mkdir()
        flavordb_data = {
            "100": {
                "flavor_profile": "sweet",
                "taste": "",
                "odor": "",
                "fooddb_flavor_profile": "",
                "super_sweet": "",
                "bitter": False,
            }
        }
        (flavordb_dir / "flavordb_scrape.json").write_text(json.dumps(flavordb_data))

        hsdb_dir = tmp_path / "HSDB"
        hsdb_dir.mkdir()
        empty_hsdb: dict = {"Annotations": {"Annotation": []}}
        (hsdb_dir / "HSDB_Odor.json").write_text(json.dumps(empty_hsdb))
        (hsdb_dir / "HSDB_Taste.json").write_text(json.dumps(empty_hsdb))

        return tmp_path

    def _make_mock_kg(self):
        """Create a mock KnowledgeGraph with chemical entities."""
        kg = MagicMock()
        entities = pd.DataFrame(
            [
                {
                    "entity_type": "chemical",
                    "common_name": "ethanol",
                    "external_ids": {"pubchem_compound": [100]},
                    "scientific_name": "",
                    "synonyms": [],
                },
            ],
            index=pd.Index(["e0"], name="foodatlas_id"),
        )
        kg.entities._entities = entities
        kg.entities._curr_eid = 1
        kg.triplets._triplets = pd.DataFrame(
            columns=["head_id", "relationship_id", "tail_id", "metadata_ids"]
        )
        kg.triplets._triplets.index.name = "foodatlas_id"
        kg.triplets._curr_tid = 1
        return kg

    def test_merge_adds_entities_and_triplets(self, tmp_path):
        data_dir = self._setup_data_dir(tmp_path)
        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_dir = str(data_dir)

        merge_flavordb(kg, settings)

        assert len(kg.entities._entities) > 1
        flavor_ents = kg.entities._entities[
            kg.entities._entities["entity_type"] == "flavor"
        ]
        assert len(flavor_ents) >= 1

    def test_merge_no_data(self, tmp_path):
        flavordb_dir = tmp_path / "FlavorDB"
        flavordb_dir.mkdir()
        (flavordb_dir / "flavordb_scrape.json").write_text("{}")

        hsdb_dir = tmp_path / "HSDB"
        hsdb_dir.mkdir()
        empty_hsdb: dict = {"Annotations": {"Annotation": []}}
        (hsdb_dir / "HSDB_Odor.json").write_text(json.dumps(empty_hsdb))
        (hsdb_dir / "HSDB_Taste.json").write_text(json.dumps(empty_hsdb))

        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_dir = str(tmp_path)

        merge_flavordb(kg, settings)
        # No new entities should be added beyond original
        assert len(kg.entities._entities) == 1
