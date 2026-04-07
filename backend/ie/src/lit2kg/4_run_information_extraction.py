"""Step 4: Information extraction using the OpenAI Batch API.

Reads information_extraction_input.tsv, splits into chunks, submits each
as a separate OpenAI batch, polls until complete, then saves raw results.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from openai import OpenAI

import src.lit2kg.information_extraction_model_config as config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_INPUT = (
    "outputs/text_parser/filtered_sentences/information_extraction_input.tsv"
)
DEFAULT_OUTPUT_DIR = "outputs/past_sentence_filtering_preds"
DEFAULT_MODEL = "gpt-5.2"
BATCH_SIZE = 50_000
POLL_INTERVAL = 60
TERMINAL = {"completed", "failed", "expired", "cancelled"}


def build_batch_jsonl(df: pd.DataFrame, model: str) -> bytes:
    """Build a JSONL byte string with one chat completion request per row."""
    lines: list[str] = []
    for idx, row in df.iterrows():
        prompt = config.PROMPT_TEMPLATE.format(sentence=row["sentence"])
        request = {
            "custom_id": str(idx),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": model,
                "temperature": config.TEMPERATURE,
                "max_completion_tokens": config.MAX_NEW_TOKENS,
                "messages": [
                    {"role": "system", "content": config.SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            },
        }
        lines.append(json.dumps(request))
    return "\n".join(lines).encode()


def download_raw_results(
    client: OpenAI,
    batch: Any,
    save_path: str,
) -> None:
    """Download raw batch output JSONL and save to disk."""
    file_id: str = batch.output_file_id
    raw: str = client.files.content(file_id).text
    Path(save_path).write_text(raw, encoding="utf-8")


def _poll_batches(client: OpenAI, batches: list[Any]) -> list[Any]:
    """Poll until all batches reach a terminal state."""
    log.info("Polling for completion ...")
    while True:
        pending = [b for b in batches if b.status not in TERMINAL]
        if not pending:
            break
        time.sleep(POLL_INTERVAL)
        batches = [
            client.batches.retrieve(b.id) if b.status not in TERMINAL else b
            for b in batches
        ]
        for b in batches:
            c = b.request_counts
            if c is not None:
                log.info(
                    "  %s  [%s]  completed=%d  failed=%d  total=%d",
                    b.id,
                    b.status,
                    c.completed,
                    c.failed,
                    c.total,
                )
            else:
                log.info("  %s  [%s]", b.id, b.status)

    failed = [b for b in batches if b.status != "completed"]
    if failed:
        msg = f"{len(failed)} batch(es) did not complete: " + ", ".join(
            f"{b.id} ({b.status})" for b in failed
        )
        raise RuntimeError(msg)
    return batches


def main() -> None:
    """Run the OpenAI Batch API information extraction pipeline."""
    parser = argparse.ArgumentParser(
        description="Run information extraction via the OpenAI Batch API",
    )
    parser.add_argument("--input_path", default=DEFAULT_INPUT)
    parser.add_argument("--output_dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--api_key", default=None)
    parser.add_argument("--num_rows", type=int, default=None)
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    api_key = args.api_key or os.environ["OPENAI_API_KEY"]
    client = OpenAI(api_key=api_key)

    log.info("Reading %s", args.input_path)
    df = pd.read_csv(
        args.input_path,
        sep="\t",
        dtype=str,
        keep_default_na=False,
    )
    if args.num_rows:
        df = df.head(args.num_rows)
    log.info("  %d sentences to process", len(df))

    chunks = [df.iloc[i : i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
    log.info(
        "Submitting %d batch(es) of up to %d rows each ...",
        len(chunks),
        BATCH_SIZE,
    )

    batches: list[Any] = []
    for i, chunk in enumerate(chunks):
        jsonl_bytes = build_batch_jsonl(chunk, args.model)
        log.info(
            "  [%d/%d] Uploading %d rows (%d bytes) ...",
            i + 1,
            len(chunks),
            len(chunk),
            len(jsonl_bytes),
        )
        batch_file = client.files.create(
            file=(
                f"batch_{i}.jsonl",
                io.BytesIO(jsonl_bytes),
                "application/jsonl",
            ),
            purpose="batch",
        )
        batch = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        log.info("    Batch ID: %s", batch.id)
        batches.append(batch)

    batches = _poll_batches(client, batches)

    today = args.date if args.date else date.today().strftime("%Y_%m_%d")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Downloading results ...")
    for i, b in enumerate(batches):
        result_path = str(output_dir / f"batch_{i}_results_{today}.jsonl")
        download_raw_results(client, b, result_path)
        log.info("  Saved batch %d results to %s", i, result_path)

    input_save_path = output_dir / f"batch_input_{today}.tsv"
    df.to_csv(input_save_path, sep="\t", index=True, index_label="custom_id")
    log.info("Saved input (%d rows) to %s", len(df), input_save_path)


if __name__ == "__main__":
    main()
