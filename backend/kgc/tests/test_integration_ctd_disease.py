"""Tests for CTD disease entity creation module."""

import pandas as pd
from src.integration.entities.disease.init_entities import (
    create_disease_entities,
    get_max_entity_id,
    parse_alt_disease_ids,
)


class TestGetMaxEntityId:
    def test_extracts_max_id(self):
        entities = pd.DataFrame({"foodatlas_id": ["e0", "e5", "e3"]})
        assert get_max_entity_id(entities) == 5

    def test_single_entity(self):
        entities = pd.DataFrame({"foodatlas_id": ["e42"]})
        assert get_max_entity_id(entities) == 42

    def test_triplet_ids(self):
        entities = pd.DataFrame({"foodatlas_id": ["t0", "t10"]})
        assert get_max_entity_id(entities) == 10


class TestParseAltDiseaseIds:
    def test_parses_mesh_and_omim(self):
        row = pd.Series(
            {
                "AltDiseaseIDs": ["MESH:D001", "OMIM:123456"],
                "external_ids": {"mesh": ["D999"]},
            }
        )
        result = parse_alt_disease_ids(row)
        assert "mesh" in result
        assert "D001" in result["mesh"]
        assert "omim" in result
        assert 123456 in result["omim"]

    def test_parses_do_ids(self):
        row = pd.Series(
            {
                "AltDiseaseIDs": ["DO:DOID:1234"],
                "external_ids": {},
            }
        )
        result = parse_alt_disease_ids(row)
        assert "diseaseontology" in result
        assert "DOID:1234" in result["diseaseontology"]

    def test_preserves_existing_external_ids(self):
        row = pd.Series(
            {
                "AltDiseaseIDs": ["MESH:D001"],
                "external_ids": {"custom_key": ["val"]},
            }
        )
        result = parse_alt_disease_ids(row)
        assert "custom_key" in result
        assert "mesh" in result


class TestCreateDiseaseEntities:
    def test_creates_entities(self):
        fa_entities = pd.DataFrame(
            {
                "foodatlas_id": ["e0", "e1"],
                "entity_type": ["food", "chemical"],
                "common_name": ["apple", "vitamin c"],
                "scientific_name": ["", ""],
                "synonyms": [[], []],
                "external_ids": [{}, {}],
            }
        )
        ctd_diseases = pd.DataFrame(
            {
                "DiseaseID": ["MESH:D001", "MESH:D002", "MESH:D003"],
                "DiseaseName": ["Cancer", "Diabetes", "Flu"],
                "Synonyms": [["tumor"], ["sugar disease"], []],
                "AltDiseaseIDs": [
                    ["MESH:D001", "OMIM:100"],
                    ["MESH:D002"],
                    ["MESH:D003"],
                ],
            }
        )
        ctd_chemdis = pd.DataFrame(
            {
                "DiseaseID": ["MESH:D001", "MESH:D002"],
            }
        )

        result = create_disease_entities(fa_entities, ctd_diseases, ctd_chemdis, 1)
        assert len(result) == 4  # 2 original + 2 disease
        diseases = result[result["entity_type"] == "disease"]
        assert len(diseases) == 2
        assert diseases.iloc[0]["foodatlas_id"] == "e2"
        assert diseases.iloc[1]["foodatlas_id"] == "e3"
        assert diseases.iloc[0]["common_name"] == "cancer"
        assert diseases.iloc[0]["synonyms"] == ["tumor"]

    def test_assigns_external_ids(self):
        fa_entities = pd.DataFrame(
            {
                "foodatlas_id": ["e0"],
                "entity_type": ["food"],
                "common_name": ["apple"],
                "scientific_name": [""],
                "synonyms": [[]],
                "external_ids": [{}],
            }
        )
        ctd_diseases = pd.DataFrame(
            {
                "DiseaseID": ["MESH:D001"],
                "DiseaseName": ["Cancer"],
                "Synonyms": [[]],
                "AltDiseaseIDs": [["MESH:D001", "OMIM:100"]],
            }
        )
        ctd_chemdis = pd.DataFrame({"DiseaseID": ["MESH:D001"]})

        result = create_disease_entities(fa_entities, ctd_diseases, ctd_chemdis, 0)
        disease = result[result["entity_type"] == "disease"].iloc[0]
        assert "mesh" in disease["external_ids"]
        assert "omim" in disease["external_ids"]
