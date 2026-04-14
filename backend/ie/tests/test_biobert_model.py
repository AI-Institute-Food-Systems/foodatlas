"""Tests for BioBERT model wrapper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import torch
from src.pipeline.filtering.biobert.model import BioBERTRunner, _SentenceDataset


def test_sentence_dataset():
    encodings = {
        "input_ids": torch.tensor([[1, 2, 3], [4, 5, 6]]),
        "attention_mask": torch.tensor([[1, 1, 1], [1, 1, 0]]),
    }
    dataset = _SentenceDataset(encodings)
    assert len(dataset) == 2
    item = dataset[0]
    assert torch.equal(item["input_ids"], torch.tensor([1, 2, 3]))
    assert torch.equal(item["attention_mask"], torch.tensor([1, 1, 1]))


def test_sentence_dataset_single():
    encodings = {
        "input_ids": torch.tensor([[10, 20]]),
    }
    dataset = _SentenceDataset(encodings)
    assert len(dataset) == 1
    assert torch.equal(dataset[0]["input_ids"], torch.tensor([10, 20]))


@patch("torch.cuda.is_available", return_value=False)
@patch("src.pipeline.filtering.biobert.model.AutoModelForSequenceClassification")
@patch("src.pipeline.filtering.biobert.model.AutoTokenizer")
def test_biobert_runner_infer(mock_tokenizer_cls, mock_model_cls, _mock_cuda):
    mock_tokenizer = MagicMock()
    mock_tokenizer_cls.from_pretrained.return_value = mock_tokenizer
    mock_tokenizer.return_value = {
        "input_ids": torch.tensor([[1, 2], [3, 4]]),
        "attention_mask": torch.tensor([[1, 1], [1, 1]]),
    }

    mock_model = MagicMock()
    mock_model_cls.from_pretrained.return_value = mock_model
    mock_model.return_value.logits = torch.tensor([[0.1, 0.9], [0.8, 0.2]])
    mock_model.eval.return_value = None
    mock_model.to.return_value = mock_model

    runner = BioBERTRunner("fake_dir", batch_size=2)
    probs = runner.infer(["sentence one", "sentence two"])

    assert len(probs) == 2
    assert probs[0] > 0.5
    assert probs[1] < 0.5
