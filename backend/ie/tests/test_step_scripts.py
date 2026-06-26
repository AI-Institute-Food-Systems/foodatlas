"""Tests for pipeline stage modules."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from src.pipeline.extraction.runner import build_batch_jsonl, load_prompt
from src.pipeline.filtering.runner import run_biobert_filter
from src.pipeline.search.runner import run_search

_SYSTEM_PATH = "src/pipeline/extraction/prompts/system/v1.txt"
_USER_PATH = "src/pipeline/extraction/prompts/user/v1.txt"


def test_run_biobert_filter_missing_column(tmp_path):
    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"col1": ["a"]}).to_csv(tsv, sep="\t", index=False)

    with pytest.raises(ValueError, match="sentence"):
        run_biobert_filter(
            input_file_path=str(tsv),
            save_file_path=str(tmp_path / "out.tsv"),
            model_dir="fake/",
        )


def test_run_biobert_filter_no_chunks(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.99]
    monkeypatch.setattr(
        "src.pipeline.filtering.runner.BioBERTRunner",
        MagicMock(return_value=mock_runner),
    )

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["Hello world"]}).to_csv(tsv, sep="\t", index=False)

    out_path = tmp_path / "out.tsv"
    run_biobert_filter(
        input_file_path=str(tsv),
        save_file_path=str(out_path),
        model_dir="fake/",
    )

    result = pd.read_csv(out_path, sep="\t")
    assert len(result) == 1
    assert "answer" in result.columns


def test_run_biobert_filter_with_chunks(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.99]
    monkeypatch.setattr(
        "src.pipeline.filtering.runner.BioBERTRunner",
        MagicMock(return_value=mock_runner),
    )

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["Hello world", "Another sentence"]}).to_csv(
        tsv, sep="\t", index=False
    )

    save_dir = tmp_path / "chunks"
    run_biobert_filter(
        input_file_path=str(tsv),
        save_file_path=str(save_dir),
        model_dir="fake/",
        chunk_size=1,
    )

    assert save_dir.exists()
    chunk_files = list(save_dir.glob("*.tsv"))
    assert len(chunk_files) == 2


def test_run_biobert_filter_with_num_data_points(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.99]
    monkeypatch.setattr(
        "src.pipeline.filtering.runner.BioBERTRunner",
        MagicMock(return_value=mock_runner),
    )

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["s1", "s2", "s3"]}).to_csv(tsv, sep="\t", index=False)

    out_path = tmp_path / "out.tsv"
    run_biobert_filter(
        input_file_path=str(tsv),
        save_file_path=str(out_path),
        model_dir="fake/",
        num_data_points=1,
    )

    result = pd.read_csv(out_path, sep="\t")
    assert len(result) == 1


def test_run_biobert_filter_chunk_resume(tmp_path, monkeypatch):
    mock_runner = MagicMock()
    mock_runner.infer.return_value = [0.5]
    monkeypatch.setattr(
        "src.pipeline.filtering.runner.BioBERTRunner",
        MagicMock(return_value=mock_runner),
    )

    tsv = tmp_path / "in.tsv"
    pd.DataFrame({"sentence": ["s1", "s2"]}).to_csv(tsv, sep="\t", index=False)

    save_dir = tmp_path / "chunks"
    save_dir.mkdir()
    (save_dir / "0000000.tsv").write_text("sentence\tanswer\ns1\t0.9\n")

    run_biobert_filter(
        input_file_path=str(tsv),
        save_file_path=str(save_dir),
        model_dir="fake/",
        chunk_size=1,
    )

    assert mock_runner.infer.call_count == 1


def test_build_batch_jsonl_content():
    df = pd.DataFrame({"sentence": ["test sentence"]})
    template = load_prompt(_USER_PATH)
    system = load_prompt(_SYSTEM_PATH)
    result = build_batch_jsonl(
        df,
        "gpt-4",
        prompt_template=template,
        system_prompt=system,
        temperature=0.0,
        max_new_tokens=512,
    )
    assert b"test sentence" in result


def test_run_search_integration(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "src.pipeline.search.runner.get_pmcid_pmid_mapping", lambda: ({}, {})
    )
    monkeypatch.setattr(
        "src.pipeline.search.runner.load_data",
        lambda fp, pmcid_pmid_dict, pmid_pmcid_dict: ({}, set()),
    )
    monkeypatch.setattr("src.pipeline.search.runner.parse_query", lambda q: ["cocoa"])
    mock_search = MagicMock(return_value={})
    monkeypatch.setattr("src.pipeline.search.runner.search_queries", mock_search)
    monkeypatch.setattr(
        "src.pipeline.search.runner._unique_pmcids_from_tsv", lambda p: {"PMC1"}
    )
    monkeypatch.setattr(
        "src.pipeline.search.runner._scan_cached_pmcids", lambda p: set()
    )
    mock_fetch = MagicMock(
        return_value=MagicMock(fetched=1, cached=0, not_in_oa=[], errors=[])
    )
    monkeypatch.setattr("src.pipeline.search.runner.fetch_missing", mock_fetch)

    query_file = tmp_path / "query_uid.tsv"
    bioc_dir = tmp_path / "bioc"
    bioc_dir.mkdir()

    run_search(
        query="cocoa",
        query_uid_results_filepath=str(query_file),
        filepath_bioc_pmc=str(bioc_dir),
        output_base_dir=str(tmp_path / "outputs"),
        current_date="2026_04_08",
    )

    mock_search.assert_called_once()
    mock_fetch.assert_called_once()
