"""Tests for OpenAI model wrapper."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from src.pipeline.extraction.openai.model import OpenAIRunner

_SYSTEM = "You are a test assistant."
_MAX_TOKENS = 100
_TEMP = 0.0


@patch("src.pipeline.extraction.openai.model.OpenAI")
def test_build_batch_line(mock_openai_cls):
    runner = OpenAIRunner(
        _SYSTEM,
        model_name="test-model",
        api_key="fake",
        max_tokens=_MAX_TOKENS,
        temperature=_TEMP,
    )
    line = runner._build_batch_line(0, "Hello")
    assert line["custom_id"] == "row_0"
    assert line["body"]["model"] == "test-model"
    assert line["body"]["messages"][1]["content"] == "Hello"


@patch("src.pipeline.extraction.openai.model.OpenAI")
def test_create_batch_file(mock_openai_cls, tmp_path):
    runner = OpenAIRunner(
        _SYSTEM,
        model_name="test-model",
        api_key="fake",
        max_tokens=_MAX_TOKENS,
        temperature=_TEMP,
    )
    path = str(tmp_path / "batch.jsonl")
    runner._create_batch_file(["prompt1", "prompt2"], path)

    lines = (tmp_path / "batch.jsonl").read_text().strip().split("\n")
    assert len(lines) == 2
    obj = json.loads(lines[0])
    assert obj["custom_id"] == "row_0"


@patch("src.pipeline.extraction.openai.model.OpenAI")
def test_infer_sync(mock_openai_cls):
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client

    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "result text"
    mock_client.chat.completions.create.return_value = mock_response

    runner = OpenAIRunner(
        _SYSTEM,
        model_name="test-model",
        api_key="fake",
        max_tokens=_MAX_TOKENS,
        temperature=_TEMP,
        use_batch=False,
        max_workers=2,
    )

    results = runner._infer_sync(["prompt1", "prompt2"])
    assert len(results) == 2
    assert all(r == "result text" for r in results)


@patch("src.pipeline.extraction.openai.model.OpenAI")
def test_infer_sync_via_infer(mock_openai_cls):
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client

    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "answer"
    mock_client.chat.completions.create.return_value = mock_response

    runner = OpenAIRunner(
        _SYSTEM,
        model_name="test-model",
        api_key="fake",
        max_tokens=_MAX_TOKENS,
        temperature=_TEMP,
        use_batch=False,
    )

    results = runner.infer(["hello"])
    assert results == ["answer"]


@patch("src.pipeline.extraction.openai.model.OpenAI")
def test_infer_batch(mock_openai_cls):
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client

    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_client.files.create.return_value = mock_file

    mock_batch = MagicMock()
    mock_batch.id = "batch-456"
    mock_batch.status = "completed"
    mock_batch.output_file_id = "file-out-789"
    mock_client.batches.create.return_value = mock_batch
    mock_client.batches.retrieve.return_value = mock_batch

    result_line = json.dumps(
        {
            "custom_id": "row_0",
            "response": {
                "body": {"choices": [{"message": {"content": "batch result"}}]}
            },
        }
    )
    mock_content = MagicMock()
    mock_content.content = result_line.encode()
    mock_client.files.content.return_value = mock_content

    runner = OpenAIRunner(
        _SYSTEM,
        model_name="test-model",
        api_key="fake",
        max_tokens=_MAX_TOKENS,
        temperature=_TEMP,
        use_batch=True,
    )

    results = runner.infer(["prompt"])
    assert len(results) == 1
    assert results[0] == "batch result"


@patch("src.pipeline.extraction.openai.model.OpenAI")
def test_upload_and_run_failed_batch(mock_openai_cls, tmp_path):
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client

    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_client.files.create.return_value = mock_file

    mock_batch = MagicMock()
    mock_batch.id = "batch-456"
    mock_batch.status = "failed"
    mock_batch.output_file_id = None
    mock_client.batches.create.return_value = mock_batch
    mock_client.batches.retrieve.return_value = mock_batch

    runner = OpenAIRunner(
        _SYSTEM,
        model_name="test",
        api_key="fake",
        max_tokens=_MAX_TOKENS,
        temperature=_TEMP,
        use_batch=True,
    )

    input_file = tmp_path / "input.jsonl"
    input_file.write_text('{"test": true}\n')

    with pytest.raises(RuntimeError, match="Batch job failed"):
        runner._upload_and_run(str(input_file), str(tmp_path / "out.jsonl"))


@patch("src.pipeline.extraction.openai.model.OpenAI")
def test_upload_and_run_none_output_file_id(mock_openai_cls, tmp_path):
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client

    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_client.files.create.return_value = mock_file

    mock_batch = MagicMock()
    mock_batch.id = "batch-456"
    mock_batch.status = "completed"
    mock_batch.output_file_id = None
    mock_client.batches.create.return_value = mock_batch
    mock_client.batches.retrieve.return_value = mock_batch

    runner = OpenAIRunner(
        _SYSTEM,
        model_name="test",
        api_key="fake",
        max_tokens=_MAX_TOKENS,
        temperature=_TEMP,
    )

    input_file = tmp_path / "input.jsonl"
    input_file.write_text('{"test": true}\n')

    with pytest.raises(RuntimeError, match="output_file_id is None"):
        runner._upload_and_run(str(input_file), str(tmp_path / "out.jsonl"))
