"""Extended tests for pubmed_search and search orchestrator modules."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pandas as pd
from src.lit2kg.pubmed_search import (
    _search_single_db,
    get_pmcid_pmid_mapping,
    save_data,
    search_queries,
)

step1 = importlib.import_module("src.lit2kg.1_search_pubmed_pmc")


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


@patch("src.lit2kg.pubmed_search.subprocess")
def test_search_single_db_pmc(mock_subprocess):
    mock_esearch = MagicMock()
    mock_esearch.stdout = MagicMock()
    mock_subprocess.Popen.return_value = mock_esearch
    mock_subprocess.check_output.return_value = "123\n456\n"
    mock_subprocess.PIPE = -1

    result = _search_single_db("pmc", "cocoa", "e@e.com", None)
    assert result == ["PMC123", "PMC456"]


@patch("src.lit2kg.pubmed_search.subprocess")
def test_search_single_db_pubmed(mock_subprocess):
    mock_esearch = MagicMock()
    mock_esearch.stdout = MagicMock()
    mock_subprocess.Popen.return_value = mock_esearch
    mock_subprocess.check_output.return_value = "789\n"
    mock_subprocess.PIPE = -1

    result = _search_single_db("pubmed", "banana", "e@e.com", "2024/01")
    assert result == ["789"]


@patch("src.lit2kg.pubmed_search.subprocess")
def test_search_single_db_with_mindate(mock_subprocess):
    mock_esearch = MagicMock()
    mock_esearch.stdout = MagicMock()
    mock_subprocess.Popen.return_value = mock_esearch
    mock_subprocess.check_output.return_value = "111\n"
    mock_subprocess.PIPE = -1

    _search_single_db("pubmed", "cocoa", "e@e.com", "2024/01/01")
    popen_args = mock_subprocess.Popen.call_args[0][0]
    assert "-mindate" in popen_args
    assert "2024/01/01" in popen_args


@patch("src.lit2kg.pubmed_search._search_single_db")
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


@patch("src.lit2kg.pubmed_search._search_single_db")
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


@patch("src.lit2kg.pubmed_search._search_single_db")
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


def test_step1_parse_argument_defaults():
    with patch("sys.argv", ["prog", "--query", "cocoa"]):
        args = step1.parse_argument()
    assert args.query == "cocoa"
    assert args.save_every == 50
