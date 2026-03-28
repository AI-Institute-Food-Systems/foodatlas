"""Tests for FlavorDB flavor description application."""

from unittest.mock import MagicMock

import pandas as pd
from src.integration.triplets.chemical_flavor.flavordb import (
    apply_flavor_descriptions,
)


class TestApplyFlavorDescriptions:
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

    def test_applies_descriptions_to_chemicals(self, tmp_path):
        dp_dir = tmp_path / "dp"
        dp_dir.mkdir()
        data = pd.DataFrame(
            [
                {
                    "_pubchem_id": 100,
                    "_flavor": "sweet",
                    "_source": "flavordb",
                    "_url": "https://example.com",
                },
                {
                    "_pubchem_id": 100,
                    "_flavor": "fruity",
                    "_source": "flavordb",
                    "_url": "https://example.com",
                },
            ]
        )
        data.to_parquet(dp_dir / "flavor_cleaned.parquet")

        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_cleaning_dir = str(dp_dir)

        apply_flavor_descriptions(kg, settings)

        descs = kg.entities._entities.at["e0", "_flavor_descriptions"]
        assert descs == ["fruity", "sweet"]

    def test_no_data_skips(self, tmp_path):
        dp_dir = tmp_path / "dp"
        dp_dir.mkdir()
        empty = pd.DataFrame(columns=["_pubchem_id", "_flavor", "_source", "_url"])
        empty.to_parquet(dp_dir / "flavor_cleaned.parquet")

        kg = self._make_mock_kg()
        settings = MagicMock()
        settings.data_cleaning_dir = str(dp_dir)

        apply_flavor_descriptions(kg, settings)
        assert "_flavor_descriptions" not in kg.entities._entities.columns
