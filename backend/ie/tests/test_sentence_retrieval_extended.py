"""Extended tests for sentence retrieval module."""

from __future__ import annotations

import json

import pandas as pd
from src.lit2kg.sentence_retrieval import retrieve_sentences


def test_retrieve_sentences_integration(tmp_path):
    bioc_dir = tmp_path / "bioc"
    bioc_dir.mkdir()

    article = {
        "documents": [
            {
                "id": "PMC100",
                "passages": [
                    {
                        "infons": {
                            "section_type": "ABSTRACT",
                            "type": "paragraph",
                        },
                        "text": (
                            "Cocoa is rich in polyphenols including "
                            "catechin and epicatechin compounds."
                        ),
                    },
                ],
            }
        ]
    }
    (bioc_dir / "PMC100.xml").write_text(json.dumps(article))

    query_uid_df = pd.DataFrame(
        {
            "pmid": ["1"],
            "pmcid": ["PMC100"],
            "queries": [str(["cocoa"])],
        }
    )
    query_uid_path = str(tmp_path / "query_uid.tsv")
    query_uid_df.to_csv(query_uid_path, sep="\t", index=False)

    foods_df = pd.DataFrame(
        {
            "query": ["cocoa"],
            "translation": [str(["cocoa"])],
        }
    )
    foods_path = str(tmp_path / "foods.tsv")
    foods_df.to_csv(foods_path, sep="\t", index=False)

    out_dir = tmp_path / "results"
    out_dir.mkdir()
    filtered_path = str(out_dir / "result_{i}.tsv")

    retrieve_sentences(
        query_uid_filepath=query_uid_path,
        filepath_bioc_pmc=str(bioc_dir),
        filepath_food_names=foods_path,
        filtered_sentences_filepath=filtered_path,
    )

    merged = out_dir / "sentence_filtering_input.tsv"
    assert merged.exists()
    result_df = pd.read_csv(merged, sep="\t")
    assert len(result_df) >= 1


def test_retrieve_sentences_no_results(tmp_path):
    bioc_dir = tmp_path / "bioc"
    bioc_dir.mkdir()

    query_uid_df = pd.DataFrame(
        {
            "pmid": ["1"],
            "pmcid": ["PMC999"],
            "queries": [str(["cocoa"])],
        }
    )
    query_uid_path = str(tmp_path / "query_uid.tsv")
    query_uid_df.to_csv(query_uid_path, sep="\t", index=False)

    foods_df = pd.DataFrame(
        {
            "query": ["cocoa"],
            "translation": [str(["cocoa"])],
        }
    )
    foods_path = str(tmp_path / "foods.tsv")
    foods_df.to_csv(foods_path, sep="\t", index=False)

    out_dir = tmp_path / "results"
    out_dir.mkdir()
    filtered_path = str(out_dir / "result_{i}.tsv")

    retrieve_sentences(
        query_uid_filepath=query_uid_path,
        filepath_bioc_pmc=str(bioc_dir),
        filepath_food_names=foods_path,
        filtered_sentences_filepath=filtered_path,
    )

    merged = out_dir / "sentence_filtering_input.tsv"
    assert merged.exists()
