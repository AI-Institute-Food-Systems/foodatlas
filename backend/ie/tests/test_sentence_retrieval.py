"""Tests for sentence_retrieval module."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from nltk.tokenize.punkt import PunktSentenceTokenizer
from src.pipeline.search.sentence_retrieval import (
    _build_translated_queries,
    _is_valid_passage,
    get_all_foods,
    get_filtered_sentences,
    pmcid_to_filepath,
)


def test_pmcid_to_filepath():
    result = pmcid_to_filepath("PMC123", "/data/BioC-PMC")
    assert str(result) == "/data/BioC-PMC/PMC123.xml"


def test_is_valid_passage_valid():
    passage = {
        "infons": {"section_type": "ABSTRACT", "type": "paragraph"},
        "text": "Some text",
    }
    assert _is_valid_passage(passage) is True


def test_is_valid_passage_no_infons():
    assert _is_valid_passage({"text": "Some text"}) is False


def test_is_valid_passage_missing_section_type():
    passage = {"infons": {"type": "paragraph"}, "text": "Some text"}
    assert _is_valid_passage(passage) is False


def test_is_valid_passage_wrong_section():
    passage = {
        "infons": {"section_type": "APPENDIX", "type": "paragraph"},
        "text": "text",
    }
    assert _is_valid_passage(passage) is False


def test_is_valid_passage_wrong_type():
    passage = {
        "infons": {"section_type": "ABSTRACT", "type": "table"},
        "text": "text",
    }
    assert _is_valid_passage(passage) is False


def test_build_translated_queries_with_dict():
    foods = {"cocoa": ["cacao", "chocolate"]}
    result = _build_translated_queries(["cocoa"], foods)
    assert result == ["cacao", "chocolate"]


def test_build_translated_queries_no_dict():
    result = _build_translated_queries(["cocoa"], {})
    assert result == ["cocoa"]


def test_build_translated_queries_mixed():
    foods = {"cocoa": ["cacao"]}
    result = _build_translated_queries(["cocoa", "apple"], foods)
    assert result == ["cacao", "apple"]


def test_get_all_foods(tmp_path):
    df = pd.DataFrame(
        {
            "query": ["cocoa", "apple"],
            "translation": [["cacao"], ["manzana"]],
        }
    )
    filepath = tmp_path / "foods.tsv"
    df.to_csv(filepath, sep="\t", index=False)
    result = get_all_foods(str(filepath))
    assert result["cocoa"] == ["cacao"]
    assert result["apple"] == ["manzana"]


def test_get_all_foods_not_found():

    with pytest.raises(FileNotFoundError):
        get_all_foods("/nonexistent/path.tsv")


def test_get_filtered_sentences_non_numeric_pmcid():
    tokenizer = PunktSentenceTokenizer()
    result = get_filtered_sentences(tokenizer, "/tmp", {}, (("", "PMCabc"), ["query"]))
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_get_filtered_sentences_missing_file():
    tokenizer = PunktSentenceTokenizer()
    result = get_filtered_sentences(
        tokenizer, "/nonexistent", {}, (("", "PMC123"), ["query"])
    )
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_get_filtered_sentences_valid(tmp_path):
    bioc_dir = tmp_path / "bioc"
    bioc_dir.mkdir()

    article = {
        "documents": [
            {
                "id": "PMC123",
                "passages": [
                    {
                        "infons": {
                            "section_type": "ABSTRACT",
                            "type": "paragraph",
                        },
                        "text": (
                            "Cocoa contains high levels of polyphenols "
                            "including catechin and epicatechin."
                        ),
                    },
                ],
            }
        ]
    }
    (bioc_dir / "PMC123.xml").write_text(json.dumps(article))

    tokenizer = PunktSentenceTokenizer()
    foods_dict = {"cocoa": ["cocoa"]}

    result = get_filtered_sentences(
        tokenizer,
        str(bioc_dir),
        foods_dict,
        (("456", "PMC123"), ["cocoa"]),
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) >= 1
    assert result.iloc[0]["pmcid"] == "PMC123"


def test_get_filtered_sentences_short_sentence(tmp_path):
    bioc_dir = tmp_path / "bioc"
    bioc_dir.mkdir()

    article = {
        "documents": [
            {
                "id": "PMC123",
                "passages": [
                    {
                        "infons": {
                            "section_type": "ABSTRACT",
                            "type": "paragraph",
                        },
                        "text": "Short.",
                    },
                ],
            }
        ]
    }
    (bioc_dir / "PMC123.xml").write_text(json.dumps(article))

    tokenizer = PunktSentenceTokenizer()
    result = get_filtered_sentences(
        tokenizer,
        str(bioc_dir),
        {"cocoa": ["cocoa"]},
        (("", "PMC123"), ["cocoa"]),
    )
    assert len(result) == 0


def test_get_filtered_sentences_no_match(tmp_path):
    bioc_dir = tmp_path / "bioc"
    bioc_dir.mkdir()

    article = {
        "documents": [
            {
                "id": "PMC123",
                "passages": [
                    {
                        "infons": {
                            "section_type": "ABSTRACT",
                            "type": "paragraph",
                        },
                        "text": (
                            "This sentence is about mathematics and "
                            "calculus, not food at all."
                        ),
                    },
                ],
            }
        ]
    }
    (bioc_dir / "PMC123.xml").write_text(json.dumps(article))

    tokenizer = PunktSentenceTokenizer()
    result = get_filtered_sentences(
        tokenizer,
        str(bioc_dir),
        {"cocoa": ["cocoa"]},
        (("", "PMC123"), ["cocoa"]),
    )
    assert len(result) == 0
