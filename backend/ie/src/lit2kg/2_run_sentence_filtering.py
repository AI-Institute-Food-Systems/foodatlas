"""Step 2: Run BioBERT sentence filtering on retrieved sentences."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from src.lit2kg.biobert.model import BioBERTRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for BioBERT sentence filtering."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file_path", required=True)
    parser.add_argument("--save_file_path", required=True)
    parser.add_argument(
        "--model_dir",
        required=True,
        help="Path to the fine-tuned BioBERT model directory.",
    )
    parser.add_argument(
        "--num_data_points",
        type=int,
        default=None,
        help="If set, only process the first N rows (for testing).",
    )
    parser.add_argument(
        "--sentence_col",
        default="sentence",
        help="Column name containing sentences (default: sentence).",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=64,
        help="BioBERT inference batch size (default: 64).",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=None,
        help="If set, save results in chunks of this many rows.",
    )
    return parser.parse_args()


def main() -> None:
    """Run BioBERT sentence filtering pipeline."""
    args = parse_args()

    df = pd.read_csv(
        args.input_file_path,
        sep="\t",
        dtype=str,
        keep_default_na=False,
    )
    if args.sentence_col not in df.columns:
        msg = f'"{args.sentence_col}" column not found. Columns: {df.columns.tolist()}'
        raise ValueError(msg)

    if args.num_data_points is not None:
        df = df.head(args.num_data_points)

    runner = BioBERTRunner(args.model_dir, batch_size=args.batch_size)

    if args.chunk_size is not None:
        save_dir = Path(args.save_file_path)
        save_dir.mkdir(parents=True, exist_ok=True)
        for start in tqdm(
            range(0, len(df), args.chunk_size),
            desc="Chunks",
            unit="chunk",
        ):
            chunk_path = save_dir / f"{start:07d}.tsv"
            if chunk_path.exists():
                log.info("Skipping chunk %d (already exists)", start)
                continue
            chunk = df.iloc[start : start + args.chunk_size].copy()
            chunk["answer"] = runner.infer(chunk[args.sentence_col].tolist())
            chunk.to_csv(chunk_path, sep="\t", index=False)
            end = min(start + args.chunk_size - 1, len(df) - 1)
            log.info(
                "Saved rows %d-%d (%d rows) -> %s",
                start,
                end,
                len(chunk),
                chunk_path,
            )
        log.info("Done. All chunks saved to %s", save_dir)
        return

    df["answer"] = runner.infer(df[args.sentence_col].tolist())
    save_path = Path(args.save_file_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path, sep="\t", index=False)
    log.info("Saved %d rows to %s", len(df), save_path)


if __name__ == "__main__":
    main()
