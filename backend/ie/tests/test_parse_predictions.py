"""Tests for extraction/parse_predictions module."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from src.pipeline.extraction.parse_predictions import (
    aggregate_batch_predictions,
    parse_response,
    tsv_to_json,
)


def test_parse_response_single_triplet():
    response = "olive, leaf, phenols, 169.10 mg/g"
    result = parse_response(response)
    assert len(result) == 1
    assert result[0] == ["olive", "leaf", "phenols", "169.10 mg/g"]


def test_parse_response_multiple_triplets():
    response = "olive, leaf, phenols, 169.10 mg/g\napple, skin, quercetin, 5.2 mg/g"
    result = parse_response(response)
    assert len(result) == 2
    assert result[0][0] == "apple"
    assert result[1][0] == "olive"


def test_parse_response_empty():
    assert parse_response("") == []
    assert parse_response("\n\n") == []


def test_parse_response_pads_short_fields():
    response = "olive, leaf, phenols"
    result = parse_response(response)
    assert len(result) == 1
    assert result[0] == ["olive", "leaf", "phenols", ""]


def test_parse_response_missing_two_fields():
    response = "olive, phenols"
    result = parse_response(response)
    assert len(result) == 1
    assert result[0] == ["olive", "phenols", "", ""]


def test_aggregate_batch_predictions(tmp_path):
    input_df = pd.DataFrame(
        {
            "custom_id": ["0", "1"],
            "pmcid": ["PMC123", "456"],
            "section": ["ABSTRACT", "RESULTS"],
            "matched_query": ["cocoa", "banana"],
            "sentence": ["Cocoa has polyphenols.", "Banana has potassium."],
            "answer": ["0.99", "0.95"],
        }
    )
    input_path = tmp_path / "batch_input.tsv"
    input_df.to_csv(input_path, sep="\t", index=False)

    results_dir = tmp_path / "results"
    results_dir.mkdir()
    result_data = [
        {
            "custom_id": "0",
            "response": {
                "body": {"choices": [{"message": {"content": "cocoa, , polyphenols,"}}]}
            },
        },
        {
            "custom_id": "1",
            "response": {
                "body": {"choices": [{"message": {"content": "banana, , potassium,"}}]}
            },
        },
    ]
    jsonl_path = results_dir / "batch_0_results_2026_01_01.jsonl"
    jsonl_path.write_text("\n".join(json.dumps(r) for r in result_data) + "\n")

    output_path = tmp_path / "output.tsv"
    aggregate_batch_predictions(str(input_path), str(results_dir), str(output_path))

    out_df = pd.read_csv(output_path, sep="\t")
    assert len(out_df) == 2
    assert out_df.iloc[0]["pmcid"] == 123
    assert out_df.iloc[1]["pmcid"] == 456


def test_aggregate_batch_no_results(tmp_path):
    input_path = tmp_path / "batch_input.tsv"
    pd.DataFrame(
        {
            "custom_id": ["0"],
            "pmcid": ["PMC1"],
            "section": ["ABSTRACT"],
            "matched_query": ["q"],
            "sentence": ["s"],
            "answer": ["0.99"],
        }
    ).to_csv(input_path, sep="\t", index=False)

    results_dir = tmp_path / "empty_results"
    results_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        aggregate_batch_predictions(
            str(input_path), str(results_dir), str(tmp_path / "out.tsv")
        )


def test_tsv_to_json(tmp_path):
    df = pd.DataFrame(
        {
            "pmcid": [123],
            "section": ["ABSTRACT"],
            "matched_query": ["cocoa"],
            "sentence": ["Cocoa has polyphenols."],
            "prob": [0.99],
            "response": ["cocoa, , polyphenols,"],
        }
    )
    tsv_path = tmp_path / "test.tsv"
    df.to_csv(tsv_path, sep="\t", index=False)

    tsv_to_json(str(tsv_path))

    json_path = tmp_path / "test.json"
    assert json_path.exists()
    data = json.loads(json_path.read_text())
    assert "0" in data
    entry = data["0"]
    assert entry["pmcid"] == 123
    assert entry["section"] == "ABSTRACT"
    assert entry["matched_query"] == "cocoa"
    assert entry["text"] == "Cocoa has polyphenols."
    assert entry["prob"] == 0.99
    assert len(entry["triplets"]) == 1
