"""Tests for src.models — ORM model registration and metadata."""

from sqlalchemy import inspect
from src.models import (
    Base,
    BaseAttestation,
    BaseEntity,
    BaseEvidence,
    BaseTriplet,
    MVChemicalDiseaseCorrelation,
    MVChemicalEntity,
    MVDiseaseEntity,
    MVFoodChemicalComposition,
    MVFoodEntity,
    MVMetadataStatistics,
    MVSearchAutoComplete,
    Relationship,
)


class TestBaseTableRegistration:
    """Verify all models are registered in Base.metadata."""

    def test_base_entities_registered(self):
        assert "base_entities" in Base.metadata.tables

    def test_base_triplets_registered(self):
        assert "base_triplets" in Base.metadata.tables

    def test_base_evidence_registered(self):
        assert "base_evidence" in Base.metadata.tables

    def test_base_attestations_registered(self):
        assert "base_attestations" in Base.metadata.tables

    def test_relationships_registered(self):
        assert "relationships" in Base.metadata.tables


class TestMVTableRegistration:
    """Verify materialized view tables are registered."""

    def test_mv_food_entities_registered(self):
        assert "mv_food_entities" in Base.metadata.tables

    def test_mv_chemical_entities_registered(self):
        assert "mv_chemical_entities" in Base.metadata.tables

    def test_mv_disease_entities_registered(self):
        assert "mv_disease_entities" in Base.metadata.tables

    def test_mv_food_chemical_composition_registered(self):
        assert "mv_food_chemical_composition" in Base.metadata.tables

    def test_mv_chemical_disease_correlation_registered(self):
        assert "mv_chemical_disease_correlation" in Base.metadata.tables

    def test_mv_search_auto_complete_registered(self):
        assert "mv_search_auto_complete" in Base.metadata.tables

    def test_mv_metadata_statistics_registered(self):
        assert "mv_metadata_statistics" in Base.metadata.tables


class TestTableNames:
    """Verify __tablename__ on each model class."""

    def test_base_entity_tablename(self):
        assert BaseEntity.__tablename__ == "base_entities"

    def test_base_triplet_tablename(self):
        assert BaseTriplet.__tablename__ == "base_triplets"

    def test_base_evidence_tablename(self):
        assert BaseEvidence.__tablename__ == "base_evidence"

    def test_base_attestation_tablename(self):
        assert BaseAttestation.__tablename__ == "base_attestations"

    def test_relationship_tablename(self):
        assert Relationship.__tablename__ == "relationships"

    def test_mv_food_entity_tablename(self):
        assert MVFoodEntity.__tablename__ == "mv_food_entities"

    def test_mv_chemical_entity_tablename(self):
        assert MVChemicalEntity.__tablename__ == "mv_chemical_entities"

    def test_mv_disease_entity_tablename(self):
        assert MVDiseaseEntity.__tablename__ == "mv_disease_entities"

    def test_mv_food_chemical_composition_tablename(self):
        assert MVFoodChemicalComposition.__tablename__ == "mv_food_chemical_composition"

    def test_mv_chemical_disease_correlation_tablename(self):
        assert (
            MVChemicalDiseaseCorrelation.__tablename__
            == "mv_chemical_disease_correlation"
        )

    def test_mv_search_auto_complete_tablename(self):
        assert MVSearchAutoComplete.__tablename__ == "mv_search_auto_complete"

    def test_mv_metadata_statistics_tablename(self):
        assert MVMetadataStatistics.__tablename__ == "mv_metadata_statistics"


class TestBaseEntityColumns:
    """Verify BaseEntity column definitions."""

    def test_primary_key(self):
        mapper = inspect(BaseEntity)
        pk_cols = [c.key for c in mapper.primary_key]
        assert pk_cols == ["foodatlas_id"]

    def test_column_names(self):
        mapper = inspect(BaseEntity)
        col_names = {c.key for c in mapper.columns}
        expected = {
            "foodatlas_id",
            "entity_type",
            "common_name",
            "scientific_name",
            "synonyms",
            "external_ids",
        }
        assert col_names == expected


class TestBaseTripletColumns:
    """Verify BaseTriplet column definitions."""

    def test_primary_key(self):
        mapper = inspect(BaseTriplet)
        pk_cols = [c.key for c in mapper.primary_key]
        assert pk_cols == ["triplet_id"]

    def test_column_names(self):
        mapper = inspect(BaseTriplet)
        col_names = {c.key for c in mapper.columns}
        expected = {
            "triplet_id",
            "head_id",
            "relationship_id",
            "tail_id",
            "source",
            "attestation_ids",
        }
        assert col_names == expected


class TestBaseAttestationColumns:
    """Verify BaseAttestation column definitions."""

    def test_primary_key(self):
        mapper = inspect(BaseAttestation)
        pk_cols = [c.key for c in mapper.primary_key]
        assert pk_cols == ["attestation_id"]

    def test_has_expected_columns(self):
        mapper = inspect(BaseAttestation)
        col_names = {c.key for c in mapper.columns}
        expected_subset = {
            "attestation_id",
            "evidence_id",
            "source",
            "conc_value",
            "conc_unit",
            "validated",
            "head_candidates",
            "tail_candidates",
        }
        assert expected_subset.issubset(col_names)


class TestRelationshipColumns:
    """Verify Relationship column definitions."""

    def test_primary_key(self):
        mapper = inspect(Relationship)
        pk_cols = [c.key for c in mapper.primary_key]
        assert pk_cols == ["foodatlas_id"]

    def test_column_names(self):
        mapper = inspect(Relationship)
        col_names = {c.key for c in mapper.columns}
        assert col_names == {"foodatlas_id", "name"}


class TestBaseEvidenceColumns:
    """Verify BaseEvidence column definitions."""

    def test_primary_key(self):
        mapper = inspect(BaseEvidence)
        pk_cols = [c.key for c in mapper.primary_key]
        assert pk_cols == ["evidence_id"]

    def test_column_names(self):
        mapper = inspect(BaseEvidence)
        col_names = {c.key for c in mapper.columns}
        assert col_names == {"evidence_id", "source_type", "reference"}


class TestMVSearchAutoCompleteColumns:
    """Verify MVSearchAutoComplete has search-specific columns."""

    def test_has_search_columns(self):
        mapper = inspect(MVSearchAutoComplete)
        col_names = {c.key for c in mapper.columns}
        assert "exact_auto" in col_names
        assert "substr_auto" in col_names
        assert "associations" in col_names
