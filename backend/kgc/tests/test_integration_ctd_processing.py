"""Tests for CTD processing module."""

from unittest.mock import MagicMock

import pandas as pd
from src.integration.entities.disease.loaders import (
    change_content_to_list,
    extract_pubmed_ids,
    filter_ctd_chemdis,
    load_ctd_data,
)


class TestLoadCtdData:
    def test_parses_header_and_data(self, tmp_path):
        content = (
            "# Comment line\n"
            "# Fields:\n"
            "# ChemicalID,DiseaseID,DirectEvidence,PubMedIDs\n"
            "D000001,MESH:D001,marker/mechanism,123|456\n"
            "D000002,MESH:D002,,789\n"
        )
        (tmp_path / "CTD_chemicals_diseases.csv").write_text(content)
        df = load_ctd_data(tmp_path, dataset="chemdis")
        assert len(df) == 2
        assert "ChemicalID" in df.columns
        assert df.iloc[0]["PubMedIDs"] == [123, 456]

    def test_loads_disease_dataset(self, tmp_path):
        content = (
            "# Comment\n"
            "# Fields:\n"
            "# DiseaseID,DiseaseName,AltDiseaseIDs,Synonyms\n"
            "MESH:D001,Cancer,DO:123|OMIM:456,tumor|neoplasm\n"
        )
        (tmp_path / "CTD_diseases.csv").write_text(content)
        df = load_ctd_data(tmp_path, dataset="disease")
        assert len(df) == 1
        assert df.iloc[0]["DiseaseName"] == "Cancer"


class TestChangeContentToList:
    def test_splits_pipe_delimited(self):
        df = pd.DataFrame({"PubMedIDs": ["123|456"]})
        result = change_content_to_list(df, columns=["PubMedIDs"])
        assert result.iloc[0]["PubMedIDs"] == [123, 456]

    def test_handles_null(self):
        df = pd.DataFrame({"PubMedIDs": [None]})
        result = change_content_to_list(df, columns=["PubMedIDs"])
        assert result.iloc[0]["PubMedIDs"] == []

    def test_preserves_non_digit_strings(self):
        df = pd.DataFrame({"Synonyms": ["cancer|MESH:D001"]})
        result = change_content_to_list(df, columns=["Synonyms"])
        assert result.iloc[0]["Synonyms"] == ["cancer", "MESH:D001"]

    def test_skips_missing_columns(self):
        df = pd.DataFrame({"other": ["val"]})
        result = change_content_to_list(df, columns=["PubMedIDs"])
        assert "other" in result.columns


class TestFilterCtdChemdis:
    def test_filters_by_evidence_and_mesh(self):
        ctd = pd.DataFrame(
            {
                "ChemicalID": ["D001", "D002", "D003"],
                "DirectEvidence": ["marker/mechanism", None, "therapeutic"],
                "DiseaseID": ["MESH:D100", "MESH:D200", "MESH:D300"],
            }
        )
        entity_store = MagicMock()
        entity_store._entities = pd.DataFrame(
            [{"external_ids": {"mesh": ["D001"]}}],
            index=pd.Index(["e0"], name="foodatlas_id"),
        )

        result = filter_ctd_chemdis(ctd, entity_store)
        assert len(result) == 1
        assert result.iloc[0]["ChemicalID"] == "D001"


class TestExtractPubmedIds:
    def test_extracts_unique_ids(self):
        ctd = pd.DataFrame(
            {
                "PubMedIDs": [[123, 456], [456, 789]],
            }
        )
        result = extract_pubmed_ids(ctd)
        assert result == {123, 456, 789}

    def test_empty_dataframe(self):
        ctd = pd.DataFrame({"PubMedIDs": [[]]})
        assert extract_pubmed_ids(ctd) == set()
