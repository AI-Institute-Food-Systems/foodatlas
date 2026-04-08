"""Run BioBERT sentence filtering on retrieved sentences."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from .biobert.model import BioBERTRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run_biobert_filter(
    *,
    input_file_path: str,
    save_file_path: str,
    model_dir: str,
    sentence_col: str = "sentence",
    batch_size: int = 64,
    chunk_size: int | None = None,
    num_data_points: int | None = None,
) -> None:
    """Run BioBERT sentence filtering pipeline."""
    df = pd.read_csv(
        input_file_path,
        sep="\t",
        dtype=str,
        keep_default_na=False,
    )
    if sentence_col not in df.columns:
        msg = f'"{sentence_col}" column not found. Columns: {df.columns.tolist()}'
        raise ValueError(msg)

    if num_data_points is not None:
        df = df.head(num_data_points)

    runner = BioBERTRunner(model_dir, batch_size=batch_size)

    if chunk_size is not None:
        save_dir = Path(save_file_path)
        save_dir.mkdir(parents=True, exist_ok=True)
        for start in tqdm(
            range(0, len(df), chunk_size),
            desc="Chunks",
            unit="chunk",
        ):
            chunk_path = save_dir / f"{start:07d}.tsv"
            if chunk_path.exists():
                log.info("Skipping chunk %d (already exists)", start)
                continue
            chunk = df.iloc[start : start + chunk_size].copy()
            chunk["answer"] = runner.infer(chunk[sentence_col].tolist())
            chunk.to_csv(chunk_path, sep="\t", index=False)
            end = min(start + chunk_size - 1, len(df) - 1)
            log.info(
                "Saved rows %d-%d (%d rows) -> %s",
                start,
                end,
                len(chunk),
                chunk_path,
            )
        log.info("Done. All chunks saved to %s", save_dir)
        return

    df["answer"] = runner.infer(df[sentence_col].tolist())
    save_path = Path(save_file_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path, sep="\t", index=False)
    log.info("Saved %d rows to %s", len(df), save_path)
