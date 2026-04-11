"""Step 5: Parse extraction TSV predictions into JSON format.

Can also aggregate OpenAI batch prediction results into the TSV format first.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def aggregate_batch_predictions(
    batch_input_path: str,
    results_dir: str,
    output_path: str,
) -> None:
    """Aggregate OpenAI batch JSONL results with the input TSV rows."""
    input_df = pd.read_csv(
        batch_input_path,
        sep="\t",
        dtype={"custom_id": str},
    )
    input_rows: dict[str, Any] = {
        str(row["custom_id"]): row for _, row in input_df.iterrows()
    }

    responses: dict[str, str] = {}
    results_path = Path(results_dir)
    result_files = sorted(results_path.glob("batch_*_results*.jsonl"))
    if not result_files:
        msg = f"No batch_*_results*.jsonl files found in {results_dir}"
        raise FileNotFoundError(msg)

    for fpath in result_files:
        with fpath.open(encoding="utf-8") as f:
            for raw_line in f:
                stripped = raw_line.strip()
                if not stripped:
                    continue
                obj = json.loads(stripped)
                custom_id = str(obj["custom_id"])
                content = obj["response"]["body"]["choices"][0]["message"]["content"]
                responses[custom_id] = content

    records: list[dict[str, object]] = []
    for custom_id in sorted(input_rows, key=int):
        row = input_rows[custom_id]
        pmcid_raw = str(row["pmcid"])
        pmcid = (
            int(pmcid_raw.removeprefix("PMC"))
            if pmcid_raw.startswith("PMC")
            else int(pmcid_raw)
        )
        records.append(
            {
                "pmcid": pmcid,
                "section": row["section"],
                "matched_query": row["matched_query"],
                "sentence": row["sentence"],
                "prob": row["answer"],
                "response": responses.get(custom_id, ""),
            }
        )

    out_df = pd.DataFrame(
        records,
        columns=[
            "pmcid",
            "section",
            "matched_query",
            "sentence",
            "prob",
            "response",
        ],
    )
    out_df.to_csv(output_path, sep="\t", index=False)
    log.info("Saved %d rows to %s", len(out_df), output_path)


def parse_response(response_str: str) -> list[list[str]]:
    """Parse a response into sorted [food, part, compound, qty] triplets."""
    triplets: list[list[str]] = []
    for raw_line in response_str.strip().splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        parts = [p.strip() for p in stripped.split(",", 3)]
        while len(parts) < 4:
            parts.append("")
        triplets.append(parts)

    triplets.sort(key=lambda t: "(" + ", ".join(t) + ")")
    return triplets


def tsv_to_json(input_path: str) -> None:
    """Convert a extraction TSV to JSON format."""
    df = pd.read_csv(input_path, sep="\t")
    output: dict[str, dict[str, object]] = {}

    for i, row in df.iterrows():
        triplets = parse_response(str(row["response"]))
        response_str = "\n".join("(" + ", ".join(t) + ")" for t in triplets)
        output[str(i)] = {
            "text": row["sentence"],
            "pmcid": int(row["pmcid"]),
            "response": response_str,
            "triplets": triplets,
        }

    output_path = input_path.replace(".tsv", ".json")
    Path(output_path).write_text(
        json.dumps(output, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Saved %d entries to %s", len(output), output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate batch predictions and/or convert extraction TSV to JSON"
        ),
    )
    parser.add_argument(
        "--batch_input_path",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--batch_results_dir",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--output_tsv",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--input_path",
        type=str,
        default=None,
    )

    args = parser.parse_args()

    if args.batch_input_path:
        if not args.batch_results_dir or not args.output_tsv:
            parser.error(
                "--batch_results_dir and --output_tsv required with --batch_input_path"
            )
        aggregate_batch_predictions(
            args.batch_input_path,
            args.batch_results_dir,
            args.output_tsv,
        )
        tsv_path = args.output_tsv
    else:
        if not args.input_path:
            parser.error("Either --batch_input_path or --input_path must be provided")
        tsv_path = args.input_path

    tsv_to_json(tsv_path)
