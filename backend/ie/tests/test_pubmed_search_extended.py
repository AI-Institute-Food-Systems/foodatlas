"""Extended tests for pubmed_search and search orchestrator modules."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.pipeline.search import runner as step1
from src.pipeline.search.pubmed_search import (
    _init_rate_limiter,
    _search_single_db,
    get_pmcid_pmid_mapping,
    save_data,
    search_queries,
)


@pytest.fixture(autouse=True)
def _setup_rate_limiter():
    _init_rate_limiter(None)


def test_get_pmcid_pmid_mapping(tmp_path):
    csv_path = tmp_path / "PMC-ids.csv"
    df = pd.DataFrame(
        {
            "PMCID": ["PMC100", "PMC200"],
            "PMID": ["1", "2"],
        }
    )
    df.to_csv(csv_path, index=False)
    pmcid_pmid, pmid_pmcid = get_pmcid_pmid_mapping(str(csv_path))
    assert pmcid_pmid["PMC100"] == "1"
    assert pmid_pmcid["2"] == "PMC200"


@patch("src.pipeline.search.pubmed_search.Entrez")
def test_search_single_db_pmc(mock_entrez):
    mock_entrez.esearch.return_value.__enter__ = lambda s: s
    mock_entrez.esearch.return_value.__exit__ = MagicMock(return_value=False)
    mock_entrez.read.return_value = {
        "Count": "2",
        "IdList": ["123", "456"],
    }

    result = _search_single_db("pmc", "cocoa", None)
    assert result == ["PMC123", "PMC456"]


@patch("src.pipeline.search.pubmed_search.Entrez")
def test_search_single_db_pubmed(mock_entrez):
    mock_entrez.esearch.return_value.__enter__ = lambda s: s
    mock_entrez.esearch.return_value.__exit__ = MagicMock(return_value=False)
    mock_entrez.read.return_value = {
        "Count": "1",
        "IdList": ["789"],
    }

    result = _search_single_db("pubmed", "banana", "2024/01")
    assert result == ["789"]


@patch("src.pipeline.search.pubmed_search.Entrez")
def test_search_single_db_with_mindate(mock_entrez):
    mock_entrez.esearch.return_value.__enter__ = lambda s: s
    mock_entrez.esearch.return_value.__exit__ = MagicMock(return_value=False)
    mock_entrez.read.return_value = {
        "Count": "1",
        "IdList": ["111"],
    }

    _search_single_db("pubmed", "cocoa", "2024/01/01")
    call_kwargs = mock_entrez.esearch.call_args[1]
    assert call_kwargs["mindate"] == "2024/01/01"


@patch("src.pipeline.search.pubmed_search.Entrez")
def test_search_single_db_empty(mock_entrez):
    mock_entrez.esearch.return_value.__enter__ = lambda s: s
    mock_entrez.esearch.return_value.__exit__ = MagicMock(return_value=False)
    mock_entrez.read.return_value = {"Count": "0", "IdList": []}

    result = _search_single_db("pubmed", "zzz_nonexistent", None)
    assert result == []


@patch("src.pipeline.search.pubmed_search._search_single_db")
def test_search_queries_basic(mock_search):
    mock_search.return_value = ["PMC100"]

    data: dict[tuple[str, str], list[str]] = {}
    prev: set[str] = set()
    pmcid_pmid = {"PMC100": "1"}
    pmid_pmcid = {"1": "PMC100"}

    result = search_queries(
        queries=["cocoa"],
        data=data,
        previous_queries=prev,
        pmcid_pmid_dict=pmcid_pmid,
        pmid_pmcid_dict=pmid_pmcid,
        email="test@test.com",
        min_date=None,
        save_every=100,
        save_filepath="/dev/null",
    )
    assert ("1", "PMC100") in result


@patch("src.pipeline.search.pubmed_search._search_single_db")
def test_search_queries_skips_previous(mock_search):
    mock_search.return_value = ["PMC100"]

    data: dict[tuple[str, str], list[str]] = {}
    prev = {"cocoa"}

    result = search_queries(
        queries=["cocoa"],
        data=data,
        previous_queries=prev,
        pmcid_pmid_dict={},
        pmid_pmcid_dict={},
        email="test@test.com",
        min_date=None,
        save_every=100,
        save_filepath="/dev/null",
    )
    assert len(result) == 0
    mock_search.assert_not_called()


@patch("src.pipeline.search.pubmed_search._search_single_db")
def test_search_queries_exception_handling(mock_search):
    mock_search.side_effect = RuntimeError("test error")

    result = search_queries(
        queries=["cocoa"],
        data={},
        previous_queries=set(),
        pmcid_pmid_dict={},
        pmid_pmcid_dict={},
        email="test@test.com",
        min_date=None,
        save_every=100,
        save_filepath="/dev/null",
    )
    assert len(result) == 0


def test_save_data_roundtrip(tmp_path):
    data = {
        ("pmid1", "PMC1"): ["q1", "q2"],
    }
    filepath = str(tmp_path / "test.tsv")
    save_data(data, filepath)
    df = pd.read_csv(filepath, sep="\t")
    assert len(df) == 1
    assert df.iloc[0]["pmid"] == "pmid1"


def test_step1_run_search_exists():
    assert callable(step1.run_search)
