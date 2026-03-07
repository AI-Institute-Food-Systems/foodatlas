"""Tests for KGC data models."""

import pytest
from src.models import (
    ChemicalEntity,
    Entity,
    FoodEntity,
    MetadataContains,
    Relationship,
    RelationshipType,
    Triplet,
)
from src.models.version import KGVersion


class TestEntity:
    def test_food_entity(self):
        e = FoodEntity(
            foodatlas_id="e1",
            common_name="apple",
        )
        assert e.entity_type == "food"
        assert e.synonyms == []
        assert e.external_ids == {}

    def test_chemical_entity(self):
        e = ChemicalEntity(
            foodatlas_id="e2",
            common_name="vitamin C",
            synonyms=["ascorbic acid"],
            external_ids={"pubchem_cid": ["54670067"]},
        )
        assert e.entity_type == "chemical"
        assert len(e.synonyms) == 1

    def test_entity_requires_type(self):
        e = Entity(
            foodatlas_id="e3",
            entity_type="food",
            common_name="banana",
        )
        assert e.entity_type == "food"

    def test_entity_invalid_type(self):
        with pytest.raises(ValueError):
            Entity(
                foodatlas_id="e4",
                entity_type="invalid",
                common_name="test",
            )


class TestTriplet:
    def test_triplet(self):
        t = Triplet(
            foodatlas_id="t1",
            head_id="e1",
            relationship_id="r1",
            tail_id="e2",
            metadata_ids=["mc1", "mc2"],
        )
        assert t.head_id == "e1"
        assert len(t.metadata_ids) == 2

    def test_triplet_defaults(self):
        t = Triplet(
            foodatlas_id="t2",
            head_id="e1",
            relationship_id="r1",
            tail_id="e2",
        )
        assert t.metadata_ids == []


class TestMetadata:
    def test_metadata_contains(self):
        m = MetadataContains(
            foodatlas_id="mc1",
            conc_value=1.5,
            conc_unit="mg",
            food_part="fruit",
            source="FDC",
            reference=["PMID:12345"],
            food_name_raw="apple",
            chemical_name_raw="vitamin C",
        )
        assert m.conc_value == 1.5
        assert m.reference == ["PMID:12345"]

    def test_metadata_defaults(self):
        m = MetadataContains(foodatlas_id="mc2")
        assert m.conc_value is None
        assert m.food_part == ""
        assert m.reference == []


class TestRelationship:
    def test_relationship_types(self):
        assert RelationshipType.CONTAINS == "r1"
        assert RelationshipType.IS_A == "r2"
        assert RelationshipType.PART_OF == "r3"
        assert RelationshipType.HAS_PROPERTY == "r4"
        assert RelationshipType.RELATED_TO == "r5"

    def test_relationship_model(self):
        r = Relationship(
            relationship_id="r1",
            relationship_type=RelationshipType.CONTAINS,
            description="food contains chemical",
        )
        assert r.relationship_type == RelationshipType.CONTAINS


class TestKGVersion:
    def test_version_string(self):
        v = KGVersion(major=2, minor=1, patch=3)
        assert v.version_string == "2.1.3"

    def test_version_string_with_label(self):
        v = KGVersion(major=1, minor=0, patch=0, label="beta")
        assert v.version_string == "1.0.0-beta"

    def test_version_defaults(self):
        v = KGVersion()
        assert v.version_string == "0.1.0"
