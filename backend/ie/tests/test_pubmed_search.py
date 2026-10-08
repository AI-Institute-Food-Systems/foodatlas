"""Tests for pubmed_search module."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
from src.models.settings import IESettings
from src.pipeline.search.pubmed_search import (
    _MAX_DATE,
    _resolve_uid,
    _search_single_db,
    load_data,
    parse_query,
    save_data,
)


def test_parse_query_single():
    result = parse_query("cocoa")
    assert result == ["cocoa"]


def test_parse_query_comma_separated():
    result = parse_query("cocoa,banana,apple")
    assert result == ["cocoa", "banana", "apple"]


def test_parse_query_from_file(tmp_path):
    query_file = tmp_path / "queries.txt"
    df = pd.DataFrame({"query": ["cocoa", "banana"]})
    df.to_csv(query_file, sep="\t", index=False)
    result = parse_query(str(query_file))
    assert result == ["cocoa", "banana"]


def test_resolve_uid_pmc():
    pmcid_pmid = {"PMC123": "456"}
    pmid_pmcid = {"456": "PMC123"}
    pmid, pmcid = _resolve_uid("PMC123", "pmc", pmcid_pmid, pmid_pmcid)
    assert pmid == "456"
    assert pmcid == "PMC123"


def test_resolve_uid_pmc_no_mapping():
    pmid, pmcid = _resolve_uid("PMC999", "pmc", {}, {})
    assert pmid == ""
    assert pmcid == "PMC999"


def test_resolve_uid_pubmed():
    pmcid_pmid = {"PMC123": "456"}
    pmid_pmcid = {"456": "PMC123"}
    pmid, pmcid = _resolve_uid("456", "pubmed", pmcid_pmid, pmid_pmcid)
    assert pmid == "456"
    assert pmcid == "PMC123"


def test_resolve_uid_pubmed_no_mapping():
    pmid, pmcid = _resolve_uid("999", "pubmed", {}, {})
    assert pmid == "999"
    assert pmcid == ""


def test_save_and_load_data(tmp_path):
    data = {
        ("123", "PMC456"): ["cocoa", "banana"],
        ("789", "PMC012"): ["apple"],
    }
    filepath = str(tmp_path / "data.tsv")
    save_data(data, filepath)

    pmcid_pmid = {"PMC456": "123", "PMC012": "789"}
    pmid_pmcid = {"123": "PMC456", "789": "PMC012"}

    loaded, prev_queries = load_data(filepath, pmcid_pmid, pmid_pmcid)
    assert ("123", "PMC456") in loaded
    assert loaded[("123", "PMC456")] == ["cocoa", "banana"]
    assert prev_queries == {"cocoa", "banana", "apple"}


def test_load_data_nonexistent_file():
    data, prev = load_data(
        "/nonexistent/path.tsv",
        {},
        {},
    )
    assert data == {}
    assert prev == set()


def test_load_data_fills_missing_pmid(tmp_path):
    df = pd.DataFrame(
        {
            "pmid": [""],
            "pmcid": ["PMC100"],
            "queries": ["['cocoa']"],
        }
    )
    filepath = str(tmp_path / "data.tsv")
    df.to_csv(filepath, sep="\t", index=False)

    pmcid_pmid = {"PMC100": "999"}
    pmid_pmcid = {"999": "PMC100"}

    loaded, _ = load_data(filepath, pmcid_pmid, pmid_pmcid)
    for key in loaded:
        assert key[0] == "999"


def test_load_data_fills_missing_pmcid(tmp_path):
    df = pd.DataFrame(
        {
            "pmid": ["999"],
            "pmcid": [""],
            "queries": ["['cocoa']"],
        }
    )
    filepath = str(tmp_path / "data.tsv")
    df.to_csv(filepath, sep="\t", index=False)

    pmcid_pmid = {"PMC100": "999"}
    pmid_pmcid = {"999": "PMC100"}

    loaded, _ = load_data(filepath, pmcid_pmid, pmid_pmcid)
    for key in loaded:
        assert key[1] == "PMC100"


class TestDateWindow:
    """IE_MAX_DATE caps the search window's end (mirrors IE_MIN_DATE's start)."""

    def test_max_date_is_passed_to_esearch(self) -> None:
        with patch(
            "src.pipeline.search.pubmed_search._collect_ids", return_value=[]
        ) as collect:
            _search_single_db("pubmed", "apple", "2026/09/11", "2026/10/02")
        assert collect.call_args.args[2:] == ("2026/09/11", "2026/10/02")

    def test_absent_max_date_falls_back_to_the_module_ceiling(self) -> None:
        with patch(
            "src.pipeline.search.pubmed_search._collect_ids", return_value=[]
        ) as collect:
            _search_single_db("pubmed", "apple", "2026/09/11")
        assert collect.call_args.args[3] == _MAX_DATE

    def test_settings_carries_the_knob(self) -> None:
        assert IESettings.model_validate({"max_date": "2026_10_02"}).max_date == (
            "2026_10_02"
        )
