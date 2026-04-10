"""Tests for filtering/aggregate module."""

from __future__ import annotations

import pandas as pd
from src.pipeline.filtering.aggregate import aggregate_food_chem_sentences, strip_pmc


def test_strip_pmc_with_prefix():
    series = pd.Series(["PMC123", "PMC456"])
    result = strip_pmc(series)
    assert list(result) == [123, 456]


def test_strip_pmc_without_prefix():
    series = pd.Series(["123", "456"])
    result = strip_pmc(series)
    assert list(result) == [123, 456]


def test_strip_pmc_mixed():
    series = pd.Series(["PMC789", "100"])
    result = strip_pmc(series)
    assert list(result) == [789, 100]


def test_aggregate_no_files(tmp_path):
    input_dir = tmp_path / "empty"
    input_dir.mkdir()
    aggregate_food_chem_sentences(
        str(input_dir),
        str(tmp_path / "agg.tsv"),
        str(tmp_path / "ie.tsv"),
        str(tmp_path / "ref"),
        0.99,
    )


def test_aggregate_with_threshold(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    df = pd.DataFrame(
        {
            "pmcid": ["PMC1", "PMC2", "PMC3"],
            "section": ["ABSTRACT"] * 3,
            "matched_query": ["q"] * 3,
            "sentence": ["s1", "s2", "s3"],
            "answer": [0.999, 0.5, 0.995],
        }
    )
    df.to_csv(input_dir / "chunk_0.tsv", sep="\t", index=False)

    ref_dir = tmp_path / "ref"
    ref_dir.mkdir()

    agg_path = tmp_path / "agg.tsv"
    ie_path = tmp_path / "ie.tsv"

    aggregate_food_chem_sentences(
        str(input_dir),
        str(agg_path),
        str(ie_path),
        str(ref_dir),
        0.99,
    )

    agg = pd.read_csv(agg_path, sep="\t")
    assert len(agg) == 2

    ie = pd.read_csv(ie_path, sep="\t")
    assert len(ie) == 2


def test_aggregate_with_reference_dedup(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    df = pd.DataFrame(
        {
            "pmcid": ["PMC100", "PMC200"],
            "section": ["ABSTRACT"] * 2,
            "matched_query": ["q"] * 2,
            "sentence": ["s1", "s2"],
            "answer": [0.999, 0.999],
        }
    )
    df.to_csv(input_dir / "chunk_0.tsv", sep="\t", index=False)

    ref_dir = tmp_path / "ref"
    ref_dir.mkdir()
    ref_df = pd.DataFrame(
        {
            "pmcid": [100],
            "section": ["ABSTRACT"],
            "matched_query": ["q"],
            "sentence": ["old"],
        }
    )
    ref_df.to_csv(ref_dir / "extraction_predicted_2025.tsv", sep="\t", index=False)

    agg_path = tmp_path / "agg.tsv"
    ie_path = tmp_path / "ie.tsv"

    aggregate_food_chem_sentences(
        str(input_dir),
        str(agg_path),
        str(ie_path),
        str(ref_dir),
        0.99,
    )

    ie = pd.read_csv(ie_path, sep="\t")
    assert len(ie) == 1
    assert str(ie.iloc[0]["pmcid"]) == "PMC200"


def test_aggregate_all_below_threshold(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    df = pd.DataFrame(
        {
            "pmcid": ["PMC1"],
            "section": ["ABSTRACT"],
            "matched_query": ["q"],
            "sentence": ["s1"],
            "answer": [0.5],
        }
    )
    df.to_csv(input_dir / "chunk_0.tsv", sep="\t", index=False)

    ref_dir = tmp_path / "ref"
    ref_dir.mkdir()

    aggregate_food_chem_sentences(
        str(input_dir),
        str(tmp_path / "agg.tsv"),
        str(tmp_path / "ie.tsv"),
        str(ref_dir),
        0.99,
    )
