"""Tests for BioBERT training module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
from src.pipeline.filtering.biobert.train import (
    BATCH_SIZE,
    DATA_ROOT,
    EPOCHS,
    LR,
    MAX_LENGTH,
    MODEL_NAME,
    compute_metrics,
    main,
    parse_args,
)


def test_constants():
    assert MODEL_NAME == "dmis-lab/biobert-v1.1"
    assert isinstance(BATCH_SIZE, int)
    assert isinstance(LR, float)
    assert isinstance(EPOCHS, int)
    assert isinstance(MAX_LENGTH, int)
    assert Path(DATA_ROOT).parts[-1] == "annotated_binary"


def test_parse_args():
    with patch("sys.argv", ["prog", "--output_dir", "/tmp/out"]):
        args = parse_args()
    assert args.output_dir == "/tmp/out"
    assert not args.production


def test_parse_args_production():
    with patch("sys.argv", ["prog", "--output_dir", "/tmp/out", "--production"]):
        args = parse_args()
    assert args.production is True


def test_compute_metrics():
    logits = np.array([[0.1, 0.9], [0.8, 0.2], [0.3, 0.7]])
    labels = np.array([1, 0, 1])

    mock_pred = MagicMock()
    mock_pred.predictions = logits
    mock_pred.label_ids = labels

    result = compute_metrics(mock_pred)
    assert "f1" in result
    assert "precision" in result
    assert "recall" in result
    assert result["f1"] == 1.0


@patch("src.pipeline.filtering.biobert.train.TrainingArguments")
@patch("src.pipeline.filtering.biobert.train.DataCollatorWithPadding")
@patch("src.pipeline.filtering.biobert.train.AutoModelForSequenceClassification")
@patch("src.pipeline.filtering.biobert.train.AutoTokenizer")
@patch("src.pipeline.filtering.biobert.train.Trainer")
@patch("src.pipeline.filtering.biobert.train.load_split")
def test_main_production(
    mock_load,
    mock_trainer_cls,
    mock_tok_cls,
    mock_model_cls,
    mock_collator_cls,
    mock_training_args_cls,
    tmp_path,
):

    mock_tokenizer = MagicMock()
    mock_tok_cls.from_pretrained.return_value = mock_tokenizer
    mock_model = MagicMock()
    mock_model_cls.from_pretrained.return_value = mock_model
    mock_load.return_value = MagicMock()

    mock_trainer = MagicMock()
    mock_trainer_cls.return_value = mock_trainer

    output_dir = str(tmp_path / "model_out")
    with patch(
        "sys.argv",
        ["prog", "--output_dir", output_dir, "--production"],
    ):
        main()

    mock_trainer.train.assert_called_once()
    mock_trainer.save_model.assert_called_once_with(output_dir)


@patch("src.pipeline.filtering.biobert.train.TrainingArguments")
@patch("src.pipeline.filtering.biobert.train.DataCollatorWithPadding")
@patch("src.pipeline.filtering.biobert.train.AutoModelForSequenceClassification")
@patch("src.pipeline.filtering.biobert.train.AutoTokenizer")
@patch("src.pipeline.filtering.biobert.train.Trainer")
@patch("src.pipeline.filtering.biobert.train.load_split")
def test_main_dev(
    mock_load,
    mock_trainer_cls,
    mock_tok_cls,
    mock_model_cls,
    mock_collator_cls,
    mock_training_args_cls,
    tmp_path,
):

    mock_tokenizer = MagicMock()
    mock_tok_cls.from_pretrained.return_value = mock_tokenizer
    mock_model = MagicMock()
    mock_model_cls.from_pretrained.return_value = mock_model
    mock_load.return_value = MagicMock()

    mock_trainer = MagicMock()
    mock_trainer.evaluate.return_value = {"test_f1": 0.95}
    mock_trainer_cls.return_value = mock_trainer

    output_dir = str(tmp_path / "model_out")
    test_file = str(tmp_path / "test.tsv")
    Path(test_file).write_text("sentence\tLabel\nhello\t1\n")

    with patch(
        "sys.argv",
        ["prog", "--output_dir", output_dir, "--test_file", test_file],
    ):
        main()

    mock_trainer.train.assert_called_once()
    mock_trainer.evaluate.assert_called_once()
