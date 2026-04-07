"""Tests for step 4: information extraction (build_batch_jsonl)."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
from src.lit2kg.information_extraction_model_config import (
    MAX_NEW_TOKENS,
    PROMPT_TEMPLATE,
    SYSTEM_PROMPT,
    TEMPERATURE,
)

step4 = importlib.import_module("src.lit2kg.4_run_information_extraction")
build_batch_jsonl = step4.build_batch_jsonl
download_raw_results = step4.download_raw_results


def test_model_config():
    assert isinstance(SYSTEM_PROMPT, str)
    assert len(SYSTEM_PROMPT) > 0
    assert isinstance(MAX_NEW_TOKENS, int)
    assert isinstance(TEMPERATURE, float)
    assert "{sentence}" in PROMPT_TEMPLATE


def test_build_batch_jsonl_single_row():
    df = pd.DataFrame(
        {
            "sentence": ["Cocoa contains polyphenols."],
        }
    )
    result = build_batch_jsonl(df, "gpt-4")
    lines = result.decode().strip().split("\n")
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["custom_id"] == "0"
    assert obj["body"]["model"] == "gpt-4"
    assert obj["body"]["temperature"] == 0.0
    assert "polyphenols" in obj["body"]["messages"][1]["content"]


def test_build_batch_jsonl_multiple_rows():
    df = pd.DataFrame(
        {
            "sentence": ["First sentence.", "Second sentence."],
        }
    )
    result = build_batch_jsonl(df, "gpt-5")
    lines = result.decode().strip().split("\n")
    assert len(lines) == 2
    for i, line in enumerate(lines):
        obj = json.loads(line)
        assert obj["custom_id"] == str(i)


def test_download_raw_results(tmp_path):
    mock_client = MagicMock()
    mock_file_content = MagicMock()
    mock_file_content.text = '{"result": "test"}\n'
    mock_client.files.content.return_value = mock_file_content

    mock_batch = MagicMock()
    mock_batch.output_file_id = "file-123"

    save_path = str(tmp_path / "results.jsonl")
    download_raw_results(mock_client, mock_batch, save_path)

    content = Path(save_path).read_text()
    assert '{"result": "test"}' in content
