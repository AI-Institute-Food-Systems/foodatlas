"""
Information extraction using the OpenAI Batch API.

Reads information_extraction_input.tsv, splits it into chunks of BATCH_SIZE
rows, submits each chunk as a separate OpenAI batch, polls all batches until
complete, then saves:
  - batch_<i>_results_YYYY_MM_DD.jsonl  raw API response lines per batch
  - batch_input_YYYY_MM_DD.tsv          input rows with custom_id index

Parsing of the raw JSONL into the final output format is handled by a
separate script.

Usage:
    python -m src.Lit2_KG.4_run_infomation_extraction \
        [--input_path  outputs/text_parser/filtered_sentences/information_extraction_input.tsv] \
        [--output_dir  outputs/past_sentence_filtering_preds] \
        [--model       gpt-5.2] \
        [--num_rows    100]           # optional: limit rows for testing
"""

import argparse
import io
import json
import os
import time
from datetime import date

import pandas as pd
from openai import OpenAI

import src.Lit2_KG.information_extraction_model_config as config

DEFAULT_INPUT      = "outputs/text_parser/filtered_sentences/information_extraction_input.tsv"
DEFAULT_OUTPUT_DIR = "outputs/past_sentence_filtering_preds"
DEFAULT_MODEL      = "gpt-5.2"
BATCH_SIZE         = 50_000
POLL_INTERVAL      = 60   # seconds between status checks
TERMINAL           = {"completed", "failed", "expired", "cancelled"}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def build_batch_jsonl(df: pd.DataFrame, model: str) -> bytes:
    """Build a JSONL byte string with one chat completion request per row."""
    lines = []
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
                    {"role": "user",   "content": prompt},
                ],
            },
        }
        lines.append(json.dumps(request))
    return "\n".join(lines).encode()


def download_raw_results(client: OpenAI, batch, save_path: str) -> None:
    """Download raw batch output JSONL and save to disk."""
    raw = client.files.content(batch.output_file_id).text
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(raw)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(
        description="Run information extraction via the OpenAI Batch API"
    )
    parser.add_argument("--input_path",  default=DEFAULT_INPUT,
                        help="Input TSV (must have pmcid/section/matched_query/sentence/answer columns).")
    parser.add_argument("--output_dir",  default=DEFAULT_OUTPUT_DIR,
                        help="Directory to save raw results and input TSV.")
    parser.add_argument("--model",       default=DEFAULT_MODEL,
                        help="OpenAI model name.")
    parser.add_argument("--api_key",     default=None,
                        help="OpenAI API key (falls back to OPENAI_API_KEY env var).")
    parser.add_argument("--num_rows",    type=int, default=None,
                        help="Only process the first N rows (for testing).")
    parser.add_argument("--date",        default=None,
                        help="Date string used in output filenames (format: YYYY_MM_DD). "
                             "Defaults to today if not set.")
    args = parser.parse_args()

    client = OpenAI(api_key=args.api_key or os.environ["OPENAI_API_KEY"])

    # ── Load input ────────────────────────────────────────────────────────────
    print(f"Reading {args.input_path}")
    df = pd.read_csv(args.input_path, sep="\t", dtype=str, keep_default_na=False)
    if args.num_rows:
        df = df.head(args.num_rows)
    print(f"  {len(df)} sentences to process")

    # ── Split and submit batches ──────────────────────────────────────────────
    chunks = [df.iloc[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
    print(f"Submitting {len(chunks)} batch(es) of up to {BATCH_SIZE:,} rows each ...")

    batches = []
    for i, chunk in enumerate(chunks):
        jsonl_bytes = build_batch_jsonl(chunk, args.model)
        print(f"  [{i+1}/{len(chunks)}] Uploading {len(chunk):,} rows ({len(jsonl_bytes):,} bytes) ...")
        batch_file = client.files.create(
            file=(f"batch_{i}.jsonl", io.BytesIO(jsonl_bytes), "application/jsonl"),
            purpose="batch",
        )
        batch = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        print(f"    Batch ID: {batch.id}")
        batches.append(batch)

    # ── Poll until all batches complete ───────────────────────────────────────
    print("Polling for completion ...")
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
            print(f"  {b.id}  [{b.status}]  "
                  f"completed={c.completed}  failed={c.failed}  total={c.total}")

    failed = [b for b in batches if b.status != "completed"]
    if failed:
        raise RuntimeError(
            f"{len(failed)} batch(es) did not complete successfully: "
            + ", ".join(f"{b.id} ({b.status})" for b in failed)
        )

    # ── Download raw results ──────────────────────────────────────────────────
    today = args.date if args.date else date.today().strftime("%Y_%m_%d")
    os.makedirs(args.output_dir, exist_ok=True)

    print("Downloading results ...")
    for i, b in enumerate(batches):
        result_path = os.path.join(args.output_dir, f"batch_{i}_results_{today}.jsonl")
        download_raw_results(client, b, result_path)
        print(f"  Saved batch {i} results to {result_path}")

    # Save input rows with DataFrame index as custom_id for downstream parsing
    input_save_path = os.path.join(args.output_dir, f"batch_input_{today}.tsv")
    df.to_csv(input_save_path, sep="\t", index=True, index_label="custom_id")
    print(f"Saved input ({len(df)} rows) to {input_save_path}")


if __name__ == "__main__":
    main()
