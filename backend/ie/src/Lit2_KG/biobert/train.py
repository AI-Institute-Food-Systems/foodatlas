"""
Fine-tune BioBERT for binary food-chemical sentence classification.

Development (train/val/test splits, saves best checkpoint by val F1):
    python -m src.Lit2_KG.biobert.train \
        --output_dir outputs/biobert_binary

Production (all data, trains full 9 epochs, saves final model):
    python -m src.Lit2_KG.biobert.train \
        --output_dir outputs/biobert_binary_prod \
        --production
"""

import argparse
import os

import numpy as np
import pandas as pd
from datasets import Dataset
from sklearn.metrics import f1_score, precision_score, recall_score
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
    Trainer,
    TrainingArguments,
)

MODEL_NAME = "dmis-lab/biobert-v1.1"
BATCH_SIZE = 32
LR = 5e-5
EPOCHS = 9
MAX_LENGTH = 512

DATA_ROOT = "data/_data/FoodAtlas/sentence_filtering/annotated_binary"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--train_file", default=os.path.join(DATA_ROOT, "train.tsv"))
    parser.add_argument("--val_file",   default=os.path.join(DATA_ROOT, "val.tsv"))
    parser.add_argument("--test_file",  default=os.path.join(DATA_ROOT, "test.tsv"))
    parser.add_argument("--all_file",   default=os.path.join(DATA_ROOT, "all.tsv"))
    parser.add_argument("--output_dir", required=True,
                        help="Directory to save the model and checkpoints.")
    parser.add_argument("--production", action="store_true",
                        help="Train on all data for the full epoch count; skip evaluation.")
    return parser.parse_args()


def load_split(path, tokenizer):
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    dataset = Dataset.from_dict({
        "text":  df["sentence"].tolist(),
        "label": df["Label"].astype(int).tolist(),
    })

    def tokenize(batch):
        return tokenizer(batch["text"], truncation=True, max_length=MAX_LENGTH)

    return dataset.map(tokenize, batched=True, remove_columns=["text"])


def compute_metrics(eval_pred):
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=-1)
    return {
        "f1":        f1_score(labels, preds),
        "precision": precision_score(labels, preds),
        "recall":    recall_score(labels, preds),
    }


def main():
    args = parse_args()

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2)

    if args.production:
        print(f"Production mode: training on all data ({args.all_file})")
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
            tokenizer=tokenizer,
            data_collator=DataCollatorWithPadding(tokenizer),
        )
        trainer.train()
        trainer.save_model(args.output_dir)
        tokenizer.save_pretrained(args.output_dir)
        print(f"Production model saved to {args.output_dir}")
    else:
        train_dataset = load_split(args.train_file, tokenizer)
        val_dataset   = load_split(args.val_file,   tokenizer)
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
            tokenizer=tokenizer,
            data_collator=DataCollatorWithPadding(tokenizer),
            compute_metrics=compute_metrics,
        )
        trainer.train()
        trainer.save_model(args.output_dir)
        tokenizer.save_pretrained(args.output_dir)
        print(f"Best model saved to {args.output_dir}")

        if os.path.exists(args.test_file):
            print("\nEvaluating on test set...")
            test_dataset = load_split(args.test_file, tokenizer)
            results = trainer.evaluate(test_dataset, metric_key_prefix="test")
            for k, v in results.items():
                print(f"  {k}: {v:.4f}" if isinstance(v, float) else f"  {k}: {v}")


if __name__ == "__main__":
    main()
