"""Tests for KGC data models."""

import pytest
from src.models import (
    ChemicalEntity,
    Entity,
    FoodEntity,
    Relationship,
    RelationshipType,
    Triplet,
)
from src.models.attestation import Attestation
from src.models.entity import DiseaseEntity
from src.models.evidence import Evidence
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

    def test_disease_entity(self):
        e = DiseaseEntity(
            foodatlas_id="e3",
            common_name="diabetes",
        )
        assert e.entity_type == "disease"

    def test_entity_invalid_type(self):
        with pytest.raises(ValueError):
            Entity(
                foodatlas_id="e4",
                entity_type="invalid",
                common_name="test",
            )

    def test_flavor_type_rejected(self):
        with pytest.raises(ValueError):
            Entity(
                foodatlas_id="e5",
                entity_type="flavor",
                common_name="sweet",
            )


class TestTriplet:
    def test_triplet(self):
        t = Triplet(
            head_id="e1",
            relationship_id="r1",
            tail_id="e2",
            attestation_ids=["at1", "at2"],
        )
        assert t.head_id == "e1"
        assert len(t.attestation_ids) == 2

    def test_triplet_defaults(self):
        t = Triplet(
            head_id="e1",
            relationship_id="r1",
            tail_id="e2",
        )
        assert t.attestation_ids == []


class TestEvidence:
    def test_evidence(self):
        ev = Evidence(
            evidence_id="ev_abc",
            source_type="pubmed",
            reference='{"pmcid": 123}',
        )
        assert ev.source_type == "pubmed"

    def test_evidence_fdc(self):
        ev = Evidence(
            evidence_id="ev_def",
            source_type="fdc",
            reference='{"url": "https://fdc.nal.usda.gov"}',
        )
        assert ev.source_type == "fdc"


class TestAttestation:
    def test_attestation(self):
        att = Attestation(
            attestation_id="at_abc",
            evidence_id="ev_abc",
            source="lit2kg:gpt-3.5-ft",
            head_name_raw="apple",
            tail_name_raw="vitamin c",
            filter_score=0.99,
        )
        assert att.source == "lit2kg:gpt-3.5-ft"
        assert att.validated is False

    def test_attestation_defaults(self):
        att = Attestation(
            attestation_id="at_def",
            evidence_id="ev_def",
            source="fdc",
        )
        assert att.conc_value is None
        assert att.validated_correct is True


class TestRelationship:
    def test_relationship_types(self):
        assert RelationshipType.CONTAINS == "r1"
        assert RelationshipType.IS_A == "r2"
        assert RelationshipType.POSITIVELY_CORRELATES_WITH == "r3"
        assert RelationshipType.NEGATIVELY_CORRELATES_WITH == "r4"

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
