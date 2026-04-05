"""Tests for src.etl.parquet_reader — reading KGC parquet files."""

from pathlib import Path

import pandas as pd
from src.etl.parquet_reader import (
    _ensure_list_col,
    _parse_json_col,
    read_attestations,
    read_entities,
    read_evidence,
    read_relationships,
    read_triplets,
)


class TestParseJsonCol:
    """Unit tests for _parse_json_col helper."""

    def test_parses_json_string(self):
        series = pd.Series(['{"a": 1}', '["x", "y"]'])
        result = _parse_json_col(series)
        assert result.iloc[0] == {"a": 1}
        assert result.iloc[1] == ["x", "y"]

    def test_passes_through_non_string(self):
        series = pd.Series([{"a": 1}, [1, 2], None])
        result = _parse_json_col(series)
        assert result.iloc[0] == {"a": 1}
        assert result.iloc[1] == [1, 2]
        assert result.iloc[2] is None

    def test_empty_series(self):
        series = pd.Series([], dtype=object)
        result = _parse_json_col(series)
        assert len(result) == 0


class TestEnsureListCol:
    """Unit tests for _ensure_list_col helper."""

    def test_list_values_pass_through(self):
        series = pd.Series([["a", "b"], ["c"]])
        result = _ensure_list_col(series)
        assert result.iloc[0] == ["a", "b"]

    def test_nan_replaced_with_empty_list(self):
        series = pd.Series([None, float("nan")])
        result = _ensure_list_col(series)
        assert result.iloc[0] == []
        assert result.iloc[1] == []

    def test_string_replaced_with_empty_list(self):
        series = pd.Series(["not a list"])
        result = _ensure_list_col(series)
        assert result.iloc[0] == []


class TestReadEntities:
    """Test read_entities with fixture parquet."""

    def test_reads_correct_shape(self, fixtures_dir: Path):
        df = read_entities(fixtures_dir)
        assert len(df) == 3
        assert "foodatlas_id" in df.columns

    def test_synonyms_are_lists(self, fixtures_dir: Path):
        df = read_entities(fixtures_dir)
        for val in df["synonyms"]:
            assert isinstance(val, list)

    def test_external_ids_are_dicts(self, fixtures_dir: Path):
        df = read_entities(fixtures_dir)
        for val in df["external_ids"]:
            assert isinstance(val, dict)

    def test_scientific_name_nan_filled(self, fixtures_dir: Path):
        df = read_entities(fixtures_dir)
        # The disease entity has scientific_name=None in fixture
        disease_row = df[df["foodatlas_id"] == "d001"].iloc[0]
        assert disease_row["scientific_name"] == ""

    def test_entity_types(self, fixtures_dir: Path):
        df = read_entities(fixtures_dir)
        types = set(df["entity_type"])
        assert types == {"food", "chemical", "disease"}


class TestReadRelationships:
    """Test read_relationships with fixture parquet."""

    def test_reads_correct_shape(self, fixtures_dir: Path):
        df = read_relationships(fixtures_dir)
        assert len(df) == 4
        assert set(df.columns) == {"foodatlas_id", "name"}

    def test_relationship_ids(self, fixtures_dir: Path):
        df = read_relationships(fixtures_dir)
        assert set(df["foodatlas_id"]) == {"r1", "r2", "r3", "r4"}


class TestReadTriplets:
    """Test read_triplets with fixture parquet."""

    def test_reads_correct_shape(self, fixtures_dir: Path):
        df = read_triplets(fixtures_dir)
        assert len(df) == 2

    def test_attestation_ids_are_lists(self, fixtures_dir: Path):
        df = read_triplets(fixtures_dir)
        for val in df["attestation_ids"]:
            assert isinstance(val, list)

    def test_source_nan_filled(self, fixtures_dir: Path):
        df = read_triplets(fixtures_dir)
        for val in df["source"]:
            assert isinstance(val, str)


class TestReadEvidence:
    """Test read_evidence with fixture parquet."""

    def test_reads_correct_shape(self, fixtures_dir: Path):
        df = read_evidence(fixtures_dir)
        assert len(df) == 2

    def test_reference_are_dicts(self, fixtures_dir: Path):
        df = read_evidence(fixtures_dir)
        for val in df["reference"]:
            assert isinstance(val, dict)

    def test_evidence_ids(self, fixtures_dir: Path):
        df = read_evidence(fixtures_dir)
        assert set(df["evidence_id"]) == {"ev001", "ev002"}


class TestReadAttestations:
    """Test read_attestations with fixture parquet."""

    def test_reads_correct_shape(self, fixtures_dir: Path):
        df = read_attestations(fixtures_dir)
        assert len(df) == 3

    def test_string_cols_nan_filled(self, fixtures_dir: Path):
        df = read_attestations(fixtures_dir)
        str_cols = [
            "head_name_raw",
            "tail_name_raw",
            "conc_unit",
            "conc_value_raw",
            "conc_unit_raw",
            "food_part",
            "food_processing",
        ]
        for col in str_cols:
            for val in df[col]:
                assert isinstance(val, str)

    def test_validated_booleans(self, fixtures_dir: Path):
        df = read_attestations(fixtures_dir)
        assert df["validated"].dtype == bool
        assert df["validated_correct"].dtype == bool

    def test_candidates_are_lists(self, fixtures_dir: Path):
        df = read_attestations(fixtures_dir)
        for col in ("head_candidates", "tail_candidates"):
            for val in df[col]:
                assert isinstance(val, list)

    def test_conc_value_nullable(self, fixtures_dir: Path):
        df = read_attestations(fixtures_dir)
        # att001 has 4.6, att003 has None
        att001 = df[df["attestation_id"] == "att001"].iloc[0]
        att003 = df[df["attestation_id"] == "att003"].iloc[0]
        assert att001["conc_value"] == 4.6
        assert pd.isna(att003["conc_value"])
