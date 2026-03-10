"""Tests for FlavorDB flavor description integration."""

from unittest.mock import MagicMock

import pandas as pd
from src.integration.triplets.chemical_flavor.flavordb import (
    apply_flavor_descriptions,
)


class TestFlavordbIntegration:
    def _setup_data_dir(self, tmp_path):
        """Create minimal cleaned parquet data."""
        dp_dir = tmp_path / "dp"
        dp_dir.mkdir()
        data = pd.DataFrame(
            [
                {
                    "_pubchem_id": 100,
                    "_flavor": "sweet",
                    "_source": "flavordb",
                    "_url": "https://cosylab.iiitd.edu.in/flavordb/"
                    "molecules_json?id=100",
                }
            ]
        )
        data.to_parquet(dp_dir / "flavor_cleaned.parquet")
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

    def test_apply_descriptions(self, tmp_path):
        data_dir = self._setup_data_dir(tmp_path)
        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_dir = str(data_dir)
        settings.data_cleaning_dir = str(data_dir / "dp")

        apply_flavor_descriptions(kg, settings)

        descs = kg.entities._entities.at["e0", "_flavor_descriptions"]
        assert descs == ["sweet"]

    def test_no_data_skips(self, tmp_path):
        dp_dir = tmp_path / "dp"
        dp_dir.mkdir()
        empty = pd.DataFrame(columns=["_pubchem_id", "_flavor", "_source", "_url"])
        empty.to_parquet(dp_dir / "flavor_cleaned.parquet")

        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_dir = str(tmp_path)
        settings.data_cleaning_dir = str(dp_dir)

        apply_flavor_descriptions(kg, settings)
        assert "_flavor_descriptions" not in kg.entities._entities.columns
