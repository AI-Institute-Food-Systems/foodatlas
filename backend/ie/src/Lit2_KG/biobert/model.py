"""BioBERT inference runner — returns class-1 probabilities."""

import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset as TorchDataset
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer


class _SentenceDataset(TorchDataset):
    def __init__(self, encodings):
        self.encodings = encodings

    def __len__(self):
        return self.encodings["input_ids"].shape[0]

    def __getitem__(self, idx):
        return {k: v[idx] for k, v in self.encodings.items()}


class BioBERTRunner:
    """Run inference with a fine-tuned BioBERT classifier.

    Returns the softmax probability for the positive class (label=1)
    for each input sentence.
    """

    def __init__(self, model_dir, batch_size=64, max_length=512):
        self._batch_size = batch_size
        self._max_length = max_length
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Loading BioBERT from {model_dir} on {self._device}")
        self._tokenizer = AutoTokenizer.from_pretrained(model_dir)
        self._model = AutoModelForSequenceClassification.from_pretrained(model_dir)
        self._model.eval()
        self._model.to(self._device)

    def infer(self, sentences):
        """Return class-1 probabilities for each sentence.

        Args:
            sentences (list[str]): Raw sentence text (no prompt formatting needed).

        Returns:
            list[float]: Probability of label=1 for each sentence.
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

        probs = []
        with torch.no_grad():
            for batch in tqdm(loader, desc="Batches", unit="batch", leave=False):
                batch = {k: v.to(self._device) for k, v in batch.items()}
                logits = self._model(**batch).logits
                p = F.softmax(logits, dim=-1)[:, 1].cpu().tolist()
                probs.extend(p)

        return probs
