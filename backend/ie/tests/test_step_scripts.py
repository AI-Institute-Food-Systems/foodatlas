"""Tests for numbered step scripts (thin orchestrators and parsers)."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

step1 = importlib.import_module("src.lit2kg.1_search_pubmed_pmc")
step2 = importlib.import_module("src.lit2kg.2_run_sentence_filtering")
step4 = importlib.import_module("src.lit2kg.4_run_information_extraction")


def test_step2_parse_args():
    with patch(
        "sys.argv",
        [
            "prog",
            "--input_file_path",
            "in.tsv",
            "--save_file_path",
            "out.tsv",
            "--model_dir",
            "model/",
        ],
    ):
        args = step2.parse_args()
    assert args.input_file_path == "in.tsv"
    assert args.batch_size == 64
    assert args.chunk_size is None


def test_step2_main_missing_column(tmp_path):

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"col1": ["a"]}).to_csv(tsv, sep="\t", index=False)

    with (
        patch(
            "sys.argv",
            [
                "prog",
                "--input_file_path",
                str(tsv),
                "--save_file_path",
                str(tmp_path / "out.tsv"),
                "--model_dir",
                "fake/",
            ],
        ),
        pytest.raises(ValueError, match="sentence"),
    ):
        step2.main()


def test_step2_main_no_chunks(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.99]
    mock_runner_cls = MagicMock(return_value=mock_runner)
    monkeypatch.setattr(step2, "BioBERTRunner", mock_runner_cls)

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["Hello world"]}).to_csv(tsv, sep="\t", index=False)

    out_path = tmp_path / "out.tsv"
    with patch(
        "sys.argv",
        [
            "prog",
            "--input_file_path",
            str(tsv),
            "--save_file_path",
            str(out_path),
            "--model_dir",
            "fake/",
        ],
    ):
        step2.main()

    result = pd.read_csv(out_path, sep="\t")
    assert len(result) == 1
    assert "answer" in result.columns


def test_step2_main_with_chunks(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.99]
    monkeypatch.setattr(step2, "BioBERTRunner", MagicMock(return_value=mock_runner))

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["Hello world", "Another sentence"]}).to_csv(
        tsv, sep="\t", index=False
    )

    save_dir = tmp_path / "chunks"
    with patch(
        "sys.argv",
        [
            "prog",
            "--input_file_path",
            str(tsv),
            "--save_file_path",
            str(save_dir),
            "--model_dir",
            "fake/",
            "--chunk_size",
            "1",
        ],
    ):
        step2.main()

    assert save_dir.exists()
    chunk_files = list(save_dir.glob("*.tsv"))
    assert len(chunk_files) == 2


def test_step2_main_with_num_data_points(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.99]
    monkeypatch.setattr(step2, "BioBERTRunner", MagicMock(return_value=mock_runner))

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["s1", "s2", "s3"]}).to_csv(tsv, sep="\t", index=False)

    out_path = tmp_path / "out.tsv"
    with patch(
        "sys.argv",
        [
            "prog",
            "--input_file_path",
            str(tsv),
            "--save_file_path",
            str(out_path),
            "--model_dir",
            "fake/",
            "--num_data_points",
            "1",
        ],
    ):
        step2.main()

    result = pd.read_csv(out_path, sep="\t")
    assert len(result) == 1


def test_step2_chunk_resume(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.5]
    monkeypatch.setattr(step2, "BioBERTRunner", MagicMock(return_value=mock_runner))

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["s1", "s2"]}).to_csv(tsv, sep="\t", index=False)

    save_dir = tmp_path / "chunks"
    save_dir.mkdir()
    (save_dir / "0000000.tsv").write_text("sentence\tanswer\ns1\t0.9\n")

    with patch(
        "sys.argv",
        [
            "prog",
            "--input_file_path",
            str(tsv),
            "--save_file_path",
            str(save_dir),
            "--model_dir",
            "fake/",
            "--chunk_size",
            "1",
        ],
    ):
        step2.main()

    assert mock_runner.infer.call_count == 1


def test_step4_build_batch_jsonl():
    df = pd.DataFrame({"sentence": ["test sentence"]})
    result = step4.build_batch_jsonl(df, "gpt-4")
    assert b"test sentence" in result


def test_step1_main(tmp_path, monkeypatch):
    monkeypatch.setattr(step1, "get_pmcid_pmid_mapping", lambda: ({}, {}))
    monkeypatch.setattr(
        step1,
        "load_data",
        lambda fp, pmcid_pmid_dict, pmid_pmcid_dict: ({}, set()),
    )
    monkeypatch.setattr(step1, "parse_query", lambda q: ["cocoa"])
    mock_search = MagicMock(return_value={})
    monkeypatch.setattr(step1, "search_queries", mock_search)
    mock_retrieve = MagicMock()
    monkeypatch.setattr(step1, "retrieve_sentences", mock_retrieve)

    query_file = tmp_path / "query_uid.tsv"
    filtered_file = tmp_path / "filtered_{i}.tsv"
    last_date = tmp_path / "last_date.txt"

    with patch(
        "sys.argv",
        [
            "prog",
            "--query",
            "cocoa",
            "--query_uid_results_filepath",
            str(query_file),
            "--filtered_sentences_filepath",
            str(filtered_file),
            "--last_search_date_filepath",
            str(last_date),
        ],
    ):
        step1.main()

    mock_search.assert_called_once()
    mock_retrieve.assert_called_once()
