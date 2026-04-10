"""BioBERT inference runner -- returns class-1 probabilities."""

from __future__ import annotations

import logging
from typing import Any

import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader
from torch.utils.data import Dataset as TorchDataset
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer

log = logging.getLogger(__name__)


class _SentenceDataset(TorchDataset):
    """Simple dataset wrapping tokenizer encodings."""

    def __init__(self, encodings: dict[str, torch.Tensor]) -> None:
        self.encodings = encodings

    def __len__(self) -> int:
        return int(self.encodings["input_ids"].shape[0])

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        return {k: v[idx] for k, v in self.encodings.items()}


class BioBERTRunner:
    """Run inference with a fine-tuned BioBERT classifier.

    Returns the softmax probability for the positive class (label=1)
    for each input sentence.
    """

    def __init__(
        self,
        model_dir: str,
        batch_size: int = 64,
        max_length: int = 512,
    ) -> None:
        self._batch_size = batch_size
        self._max_length = max_length
        self._device = torch.device(
            "cuda" if torch.cuda.is_available() else "cpu",
        )
        log.info("Loading BioBERT from %s on %s", model_dir, self._device)
        self._tokenizer: Any = AutoTokenizer.from_pretrained(model_dir)
        self._model: Any = AutoModelForSequenceClassification.from_pretrained(
            model_dir,
        )
        self._model.eval()
        self._model.to(self._device)

    def infer(self, sentences: list[str]) -> list[float]:
        """Return class-1 probabilities for each sentence.

        Args:
            sentences: Raw sentence text (no prompt formatting needed).

        Returns:
            Probability of label=1 for each sentence.
        """
        encodings = self._tokenizer(
            sentences,
            truncation=True,
            max_length=self._max_length,
            padding=True,
            return_tensors="pt",
        )
        dataset = _SentenceDataset(encodings)
        loader = DataLoader(dataset, batch_size=self._batch_size)

        probs: list[float] = []
        with torch.no_grad():
            for raw_batch in tqdm(
                loader,
                desc="Batches",
                unit="batch",
                leave=False,
            ):
                device_batch = {k: v.to(self._device) for k, v in raw_batch.items()}
                logits = self._model(**device_batch).logits
                p = F.softmax(logits, dim=-1)[:, 1].cpu().tolist()
                probs.extend(p)

        return probs
