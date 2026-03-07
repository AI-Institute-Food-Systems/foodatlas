"""Tests for FlavorDB entity and triplet creation."""

import pandas as pd
from src.integration.flavordb.flavor_entities import (
    create_flavor_entities,
    create_flavor_triplets,
)


class TestCreateFlavorEntities:
    def test_creates_entities_with_ids(self):
        metadata = pd.DataFrame({"_flavor": ["sweet", "sour", "sweet"]})
        result = create_flavor_entities(metadata, 5)
        assert len(result) == 2  # unique flavors
        assert result.iloc[0]["foodatlas_id"] == "e6"
        assert result.iloc[1]["foodatlas_id"] == "e7"
        assert set(result["entity_type"]) == {"flavor"}

    def test_entity_fields(self):
        metadata = pd.DataFrame({"_flavor": ["sweet"]})
        result = create_flavor_entities(metadata, 0)
        row = result.iloc[0]
        assert row["common_name"] == "sweet"
        assert row["scientific_name"] == ""
        assert row["synonyms"] == []
        assert row["external_ids"] == {}


class TestCreateFlavorTriplets:
    def test_creates_triplets(self):
        metadata = pd.DataFrame(
            {
                "_pubchem_id": [100, 100],
                "_flavor": ["sweet", "sour"],
                "foodatlas_id": ["mf1", "mf2"],
            }
        )
        entities = pd.DataFrame(
            {
                "foodatlas_id": ["e0", "e5", "e6"],
                "entity_type": ["chemical", "flavor", "flavor"],
                "common_name": ["ethanol", "sweet", "sour"],
                "external_ids": [
                    {"pubchem_compound": [100]},
                    {},
                    {},
                ],
            }
        )

        result = create_flavor_triplets(metadata, entities, 0)
        assert len(result) == 2
        assert result.iloc[0]["head_id"] == "e0"
        assert result.iloc[0]["relationship_id"] == "r5"
        assert result.iloc[0]["foodatlas_id"] == "t1"
        assert result.iloc[0]["metadata_ids"] == ["mf1"]

    def test_skips_unmapped_chemicals(self):
        metadata = pd.DataFrame(
            {
                "_pubchem_id": [999],
                "_flavor": ["sweet"],
                "foodatlas_id": ["mf1"],
            }
        )
        entities = pd.DataFrame(
            {
                "foodatlas_id": ["e5"],
                "entity_type": ["flavor"],
                "common_name": ["sweet"],
                "external_ids": [{}],
            }
        )

        result = create_flavor_triplets(metadata, entities, 0)
        assert result.empty

    def test_empty_metadata(self):
        metadata = pd.DataFrame(
            {
                "_pubchem_id": pd.Series(dtype=int),
                "_flavor": pd.Series(dtype=str),
                "foodatlas_id": pd.Series(dtype=str),
            }
        )
        entities = pd.DataFrame(
            {
                "foodatlas_id": ["e0"],
                "entity_type": ["chemical"],
                "common_name": ["test"],
                "external_ids": [{"pubchem_compound": [100]}],
            }
        )

        result = create_flavor_triplets(metadata, entities, 0)
        assert result.empty
