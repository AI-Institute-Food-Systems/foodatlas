"""Tests for CTD PMID mapping module."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.integration.triplets.chemical_disease.ctd import (
    fetch_pmid_to_pmcid,
    get_or_create_pmid_mapping,
    load_pmid_to_pmcid,
)


class TestLoadPmidToPmcid:
    def test_loads_and_parses(self, tmp_path):
        csv_path = tmp_path / "mapping.csv"
        csv_path.write_text("pmid,pmcid\n123,PMC456\n789,PMC101\n")
        result = load_pmid_to_pmcid(csv_path)
        assert result == {123: 456, 789: 101}

    def test_skips_null_pmcid(self, tmp_path):
        csv_path = tmp_path / "mapping.csv"
        csv_path.write_text("pmid,pmcid\n123,PMC456\n789,\n")
        result = load_pmid_to_pmcid(csv_path)
        assert result == {123: 456}


class TestFetchPmidToPmcid:
    @patch("src.integration.triplets.chemical_disease.ctd.requests.get")
    def test_fetches_mappings(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = (
            b'<pmcids><record pmid="123" pmcid="PMC456"/>'
            b'<record pmid="789" pmcid="PMC101"/></pmcids>'
        )
        mock_get.return_value = mock_response

        result = fetch_pmid_to_pmcid([123, 789], "test@test.com")
        assert len(result) == 2
        assert "pmid" in result.columns
        assert "pmcid" in result.columns

    @patch("src.integration.triplets.chemical_disease.ctd.requests.get")
    def test_handles_error_status(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = fetch_pmid_to_pmcid([123], "test@test.com")
        assert result.empty


class TestGetOrCreatePmidMapping:
    def test_cache_hit(self, tmp_path):
        csv_path = tmp_path / "CTD_pubmed_ids_to_pmcid.csv"
        csv_path.write_text("pmid,pmcid\n123,PMC456\n")

        result = get_or_create_pmid_mapping(tmp_path, [123], "test@test.com")
        assert result == {123: 456}

    @patch("src.integration.triplets.chemical_disease.ctd.fetch_pmid_to_pmcid")
    def test_cache_miss_creates(self, mock_fetch, tmp_path):
        mock_fetch.return_value = pd.DataFrame(
            {
                "pmid": ["123"],
                "pmcid": ["PMC456"],
            }
        )

        result = get_or_create_pmid_mapping(tmp_path, [123], "test@test.com")
        assert result == {123: 456}
        assert (tmp_path / "CTD_pubmed_ids_to_pmcid.csv").exists()
