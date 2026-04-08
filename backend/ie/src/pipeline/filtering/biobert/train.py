"""Fine-tune BioBERT for binary food-chemical sentence classification.

Development (train/val/test splits, saves best checkpoint by val F1):
    python -m src.pipeline.filtering.biobert.train --output_dir outputs/biobert_binary

Production (all data, trains full 9 epochs, saves final model):
    python -m src.pipeline.filtering.biobert.train \
        --output_dir outputs/biobert_binary_prod --production
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from datasets import Dataset
from sklearn.metrics import f1_score, precision_score, recall_score
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
    EvalPrediction,
    Trainer,
    TrainingArguments,
)

log = logging.getLogger(__name__)

MODEL_NAME = "dmis-lab/biobert-v1.1"
BATCH_SIZE = 32
LR = 5e-5
EPOCHS = 9
MAX_LENGTH = 512

DATA_ROOT = "data/_data/FoodAtlas/sentence_filtering/annotated_binary"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for BioBERT training."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--train_file",
        default=str(Path(DATA_ROOT) / "train.tsv"),
    )
    parser.add_argument(
        "--val_file",
        default=str(Path(DATA_ROOT) / "val.tsv"),
    )
    parser.add_argument(
        "--test_file",
        default=str(Path(DATA_ROOT) / "test.tsv"),
    )
    parser.add_argument(
        "--all_file",
        default=str(Path(DATA_ROOT) / "all.tsv"),
    )
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--production", action="store_true")
    return parser.parse_args()


def load_split(path: str, tokenizer: Any) -> Dataset:
    """Load a TSV split and tokenize it for training."""
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    dataset = Dataset.from_dict(
        {
            "text": df["sentence"].tolist(),
            "label": df["Label"].astype(int).tolist(),
        }
    )

    def tokenize(batch: dict[str, list[str]]) -> dict[str, Any]:
        result: dict[str, Any] = tokenizer(
            batch["text"],
            truncation=True,
            max_length=MAX_LENGTH,
        )
        return result

    return dataset.map(tokenize, batched=True, remove_columns=["text"])


def compute_metrics(
    eval_pred: EvalPrediction,
) -> dict[str, float]:
    """Compute F1, precision, and recall for evaluation."""
    logits, labels = eval_pred.predictions, eval_pred.label_ids
    preds = np.argmax(logits, axis=-1)
    return {
        "f1": float(f1_score(labels, preds)),
        "precision": float(precision_score(labels, preds)),
        "recall": float(recall_score(labels, preds)),
    }


def main() -> None:
    """Run BioBERT fine-tuning pipeline."""
    args = parse_args()

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME,
        num_labels=2,
    )

    if args.production:
        log.info("Production mode: training on all data (%s)", args.all_file)
        train_dataset = load_split(args.all_file, tokenizer)
        training_args = TrainingArguments(
            output_dir=args.output_dir,
            num_train_epochs=EPOCHS,
            per_device_train_batch_size=BATCH_SIZE,
            learning_rate=LR,
            eval_strategy="no",
            save_strategy="no",
            logging_strategy="epoch",
            fp16=True,
            dataloader_num_workers=4,
        )
        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=train_dataset,
            processing_class=tokenizer,
            data_collator=DataCollatorWithPadding(tokenizer),
        )
        trainer.train()
        trainer.save_model(args.output_dir)
        tokenizer.save_pretrained(args.output_dir)
        log.info("Production model saved to %s", args.output_dir)
    else:
        train_dataset = load_split(args.train_file, tokenizer)
        val_dataset = load_split(args.val_file, tokenizer)
        training_args = TrainingArguments(
            output_dir=args.output_dir,
            num_train_epochs=EPOCHS,
            per_device_train_batch_size=BATCH_SIZE,
            per_device_eval_batch_size=BATCH_SIZE,
            learning_rate=LR,
            eval_strategy="epoch",
            save_strategy="epoch",
            load_best_model_at_end=True,
            metric_for_best_model="f1",
            greater_is_better=True,
            logging_strategy="epoch",
            fp16=True,
            dataloader_num_workers=4,
        )
        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=val_dataset,
            processing_class=tokenizer,
            data_collator=DataCollatorWithPadding(tokenizer),
            compute_metrics=compute_metrics,
        )
        trainer.train()
        trainer.save_model(args.output_dir)
        tokenizer.save_pretrained(args.output_dir)
        log.info("Best model saved to %s", args.output_dir)

        if Path(args.test_file).exists():
            log.info("Evaluating on test set...")
            test_dataset = load_split(args.test_file, tokenizer)
            results = trainer.evaluate(
                test_dataset,
                metric_key_prefix="test",
            )
            for k, v in results.items():
                log.info("  %s: %.4f", k, v)


if __name__ == "__main__":
    main()
