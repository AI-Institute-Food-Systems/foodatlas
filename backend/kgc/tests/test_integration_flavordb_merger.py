"""Tests for FlavorDB entity initialization and triplet merging."""

import json
from unittest.mock import MagicMock

import pandas as pd
from src.integration.entities.flavor.init_entities import append_flavors_from_flavordb
from src.integration.triplets.chemical_flavor.flavordb import merge_flavordb_triplets


class TestFlavordbIntegration:
    def _setup_data_dir(self, tmp_path):
        """Create minimal test data: cleaned parquet + raw data for triplets."""
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

        dp_dir = tmp_path / "dp"
        dp_dir.mkdir()
        metadata = pd.DataFrame(
            [
                {
                    "foodatlas_id": "mf1",
                    "source": "flavordb",
                    "reference": {
                        "url": "https://cosylab.iiitd.edu.in/flavordb/"
                        "molecules_json?id=100"
                    },
                    "_flavor": "sweet",
                    "_pubchem_id": 100,
                }
            ]
        )
        metadata.to_parquet(dp_dir / "flavor_metadata_cleaned.parquet")

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

    def test_append_flavors_adds_entities(self, tmp_path):
        data_dir = self._setup_data_dir(tmp_path)
        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_dir = str(data_dir)
        settings.data_cleaning_dir = str(data_dir / "dp")

        append_flavors_from_flavordb(kg.entities, settings)

        assert len(kg.entities._entities) > 1
        flavor_ents = kg.entities._entities[
            kg.entities._entities["entity_type"] == "flavor"
        ]
        assert len(flavor_ents) >= 1

    def test_merge_triplets_after_entities(self, tmp_path):
        data_dir = self._setup_data_dir(tmp_path)
        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_dir = str(data_dir)
        settings.data_cleaning_dir = str(data_dir / "dp")

        append_flavors_from_flavordb(kg.entities, settings)
        merge_flavordb_triplets(kg, settings)

    def test_no_data_skips(self, tmp_path):
        dp_dir = tmp_path / "dp"
        dp_dir.mkdir()
        empty_meta = pd.DataFrame(
            columns=[
                "foodatlas_id",
                "source",
                "reference",
                "_flavor",
                "_pubchem_id",
            ]
        )
        empty_meta.to_parquet(dp_dir / "flavor_metadata_cleaned.parquet")

        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_dir = str(tmp_path)
        settings.data_cleaning_dir = str(dp_dir)

        append_flavors_from_flavordb(kg.entities, settings)
        assert len(kg.entities._entities) == 1
