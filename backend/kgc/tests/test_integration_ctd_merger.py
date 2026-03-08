"""Tests for CTD merger module."""

import pandas as pd
from src.integration.triplets.chemical_disease.ctd import (
    build_chemical_id_map,
    build_disease_id_map,
    create_disease_triplets_metadata,
)


class TestBuildChemicalIdMap:
    def test_maps_mesh_ids(self):
        entities = pd.DataFrame(
            [
                {"entity_type": "chemical", "external_ids": {"mesh": ["D001", "D002"]}},
                {"entity_type": "food", "external_ids": {"mesh": ["D999"]}},
            ],
            index=pd.Index(["e0", "e1"], name="foodatlas_id"),
        )
        result = build_chemical_id_map(entities)
        assert result == {"D001": ["e0"], "D002": ["e0"]}

    def test_empty_when_no_mesh(self):
        entities = pd.DataFrame(
            [{"entity_type": "chemical", "external_ids": {}}],
            index=pd.Index(["e0"], name="foodatlas_id"),
        )
        assert build_chemical_id_map(entities) == {}


class TestBuildDiseaseIdMap:
    def test_maps_mesh_and_omim(self):
        entities = pd.DataFrame(
            [
                {
                    "entity_type": "disease",
                    "external_ids": {"mesh": ["D100"], "omim": [200]},
                },
            ],
            index=pd.Index(["e5"], name="foodatlas_id"),
        )
        result = build_disease_id_map(entities)
        assert result["MESH:D100"] == ["e5"]
        assert result["OMIM:200"] == ["e5"]

    def test_skips_non_disease(self):
        entities = pd.DataFrame(
            [{"entity_type": "chemical", "external_ids": {"mesh": ["D100"]}}],
            index=pd.Index(["e0"], name="foodatlas_id"),
        )
        assert build_disease_id_map(entities) == {}


class TestCreateDiseaseTripletMetadata:
    def test_creates_triplets_and_metadata(self):
        ctd_chemdis = pd.DataFrame(
            {
                "ChemicalID": ["D001", "D001"],
                "DiseaseID": ["MESH:D100", "MESH:D100"],
                "DirectEvidence": ["marker/mechanism", "therapeutic"],
                "PubMedIDs": [[123], [456]],
            }
        )
        fa_entities = pd.DataFrame(
            [
                {"entity_type": "chemical", "external_ids": {"mesh": ["D001"]}},
                {"entity_type": "disease", "external_ids": {"mesh": ["D100"]}},
            ],
            index=pd.Index(["e0", "e5"], name="foodatlas_id"),
        )
        pmid_to_pmcid = {123: 999}

        triplets, metadata = create_disease_triplets_metadata(
            ctd_chemdis, fa_entities, pmid_to_pmcid, 0
        )
        assert len(triplets) == 2
        assert len(metadata) == 2
        assert metadata.iloc[0]["source"] == "ctd"
        assert "_chemical_name" in metadata.columns
        assert "_disease_name" in metadata.columns

    def test_explode_creates_multiple_rows(self):
        ctd_chemdis = pd.DataFrame(
            {
                "ChemicalID": ["D001"],
                "DiseaseID": ["MESH:D100"],
                "DirectEvidence": ["marker/mechanism"],
                "PubMedIDs": [[123, 456]],
            }
        )
        fa_entities = pd.DataFrame(
            [
                {"entity_type": "chemical", "external_ids": {"mesh": ["D001"]}},
                {"entity_type": "disease", "external_ids": {"mesh": ["D100"]}},
            ],
            index=pd.Index(["e0", "e5"], name="foodatlas_id"),
        )

        triplets, metadata = create_disease_triplets_metadata(
            ctd_chemdis, fa_entities, {}, 0
        )
        assert len(metadata) == 2
        assert len(triplets) == 1
        assert len(triplets.iloc[0]["metadata_ids"]) == 2

    def test_relationship_mapping(self):
        ctd_chemdis = pd.DataFrame(
            {
                "ChemicalID": ["D001"],
                "DiseaseID": ["MESH:D100"],
                "DirectEvidence": ["therapeutic"],
                "PubMedIDs": [[123]],
            }
        )
        fa_entities = pd.DataFrame(
            [
                {"entity_type": "chemical", "external_ids": {"mesh": ["D001"]}},
                {"entity_type": "disease", "external_ids": {"mesh": ["D100"]}},
            ],
            index=pd.Index(["e0", "e5"], name="foodatlas_id"),
        )

        triplets, _ = create_disease_triplets_metadata(ctd_chemdis, fa_entities, {}, 0)
        assert triplets.iloc[0]["relationship_id"] == "r4"
