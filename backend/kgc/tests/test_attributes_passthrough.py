"""Enrichment reads and rewrites typed attributes; keys recorded by ingest
(FDC data type, NDB number, USDA FoodOn mapping) must survive that round trip."""

from __future__ import annotations

import pandas as pd
from src.models.attributes import ChemicalAttributes, FoodAttributes
from src.pipeline.enrichment.utils import read_attributes, write_attributes

SOURCE_KEYS = {
    "fdc_data_type": "sr_legacy_food",
    "ndb_number": "01001",
    "fdc_category": "Dairy and Egg Products",
    "foodon_terms": ["http://purl.obolibrary.org/obo/FOODON_03302103"],
}


def test_food_groups_update_keeps_ingest_metadata() -> None:
    entities = pd.DataFrame({"attributes": [dict(SOURCE_KEYS)]}, index=["e1"])

    attrs = read_attributes(entities, "e1", FoodAttributes)
    attrs.food_groups = ["dairy"]
    write_attributes(entities, "e1", attrs)

    assert entities.at["e1", "attributes"] == {**SOURCE_KEYS, "food_groups": ["dairy"]}


def test_models_still_default_to_their_own_fields() -> None:
    assert FoodAttributes().model_dump() == {"food_groups": []}
    assert ChemicalAttributes().model_dump() == {
        "chemical_groups": [],
        "flavor_descriptors": [],
    }
