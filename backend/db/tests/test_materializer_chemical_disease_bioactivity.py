"""Tests for src.etl.materializer_chemical_disease_bioactivity."""

import pandas as pd
from src.etl.materializer_bioactivity_bridge import disease_mesh_map
from src.etl.materializer_chemical_disease_bioactivity import (
    _as_list,
    _target_genes_per_pair,
)


class TestAsList:
    def test_list_passthrough(self):
        assert _as_list(["a", "b"]) == ["a", "b"]

    def test_none_returns_empty(self):
        assert _as_list(None) == []

    def test_nan_returns_empty(self):
        assert _as_list(float("nan")) == []


class TestDiseaseMeshMap:
    def test_maps_mesh_to_disease_id(self):
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "d1",
                    "entity_type": "disease",
                    "external_ids": {"ctd": ["D001"]},
                },
                {
                    "foodatlas_id": "c1",
                    "entity_type": "chemical",
                    "external_ids": {"ctd": ["D999"]},
                },
            ]
        )
        assert disease_mesh_map(entities) == {"D001": "d1"}

    def test_ignores_non_dict_external_ids(self):
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "d1",
                    "entity_type": "disease",
                    "external_ids": None,
                }
            ]
        )
        assert disease_mesh_map(entities) == {}


class TestTargetGenesPerPair:
    def test_collects_distinct_genes(self):
        evidence = pd.DataFrame(
            [
                {
                    "chemical_foodatlas_id": "c1",
                    "disease_foodatlas_id": "d1",
                    "bioactivity_disease_metadata_id": ["bdm1", "bdm2"],
                },
                {
                    "chemical_foodatlas_id": "c1",
                    "disease_foodatlas_id": "d1",
                    "bioactivity_disease_metadata_id": ["bdm3"],
                },
            ]
        )
        target_map = {
            "bdm1": ["GENE_A", "GENE_B"],
            "bdm2": ["GENE_B"],
            "bdm3": ["GENE_C"],
        }
        s = _target_genes_per_pair(
            evidence,
            target_map,
            ["chemical_foodatlas_id", "disease_foodatlas_id"],
        )
        assert set(s.iloc[0]) == {"GENE_A", "GENE_B", "GENE_C"}
