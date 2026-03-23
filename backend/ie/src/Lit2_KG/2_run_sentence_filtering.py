import argparse
import os

import pandas as pd
from tqdm import tqdm

from src.Lit2_KG.biobert.model import BioBERTRunner


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file_path", required=True)
    parser.add_argument("--save_file_path",  required=True)
    parser.add_argument("--model_dir", required=True,
                        help="Path to the fine-tuned BioBERT model directory.")
    parser.add_argument("--num_data_points", type=int, default=None,
                        help="If set, only process the first N rows (for testing).")
    parser.add_argument("--sentence_col", default="sentence",
                        help="Column name containing sentences (default: sentence).")
    parser.add_argument("--batch_size", type=int, default=64,
                        help="BioBERT inference batch size (default: 64).")
    parser.add_argument("--chunk_size", type=int, default=None,
                        help="If set, save results in chunks of this many rows. "
                             "save_file_path is treated as a directory. "
                             "Already-saved chunks are skipped (supports resume).")
    return parser.parse_args()


def main():
    args = parse_args()

    df = pd.read_csv(args.input_file_path, sep="\t", dtype=str, keep_default_na=False)
    if args.sentence_col not in df.columns:
        raise ValueError(f'"{args.sentence_col}" column not found. Columns: {df.columns.tolist()}')

    if args.num_data_points is not None:
        df = df.head(args.num_data_points)

    runner = BioBERTRunner(args.model_dir, batch_size=args.batch_size)

    if args.chunk_size is not None:
        save_dir = args.save_file_path
        os.makedirs(save_dir, exist_ok=True)
        for start in tqdm(range(0, len(df), args.chunk_size), desc="Chunks", unit="chunk"):
            chunk_path = os.path.join(save_dir, f"{start:07d}.tsv")
            if os.path.exists(chunk_path):
                print(f"Skipping chunk {start} (already exists)")
                continue
            chunk = df.iloc[start:start + args.chunk_size].copy()
            chunk["answer"] = runner.infer(chunk[args.sentence_col].tolist())
            chunk.to_csv(chunk_path, sep="\t", index=False)
            end = min(start + args.chunk_size - 1, len(df) - 1)
            print(f"Saved rows {start}–{end} ({len(chunk)} rows) → {chunk_path}")
        print(f"Done. All chunks saved to {save_dir}")
        return

    df["answer"] = runner.infer(df[args.sentence_col].tolist())
    os.makedirs(os.path.dirname(os.path.abspath(args.save_file_path)), exist_ok=True)
    df.to_csv(args.save_file_path, sep="\t", index=False)
    print(f"Saved {len(df)} rows to {args.save_file_path}")


if __name__ == "__main__":
    main()
