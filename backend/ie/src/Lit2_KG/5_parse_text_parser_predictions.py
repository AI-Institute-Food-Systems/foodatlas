"""
Parse a text_parser TSV prediction file into the JSON format used by model outputs.
Can also aggregate OpenAI batch prediction results into the TSV format first.

Usage (aggregate batches then convert to JSON):
    python src/Lit2_KG/parse_text_parser_predictions.py \
        --batch_input_path data/past_sentence_filtering_preds/prediction_batch/batch_input_2026_02_26.tsv \
        --batch_results_dir data/past_sentence_filtering_preds/prediction_batch \
        --output_tsv data/past_sentence_filtering_preds/text_parser_predicted_2026_02_26.tsv \
        --model_name gpt5

Usage (convert existing TSV to JSON only):
    python src/Lit2_KG/parse_text_parser_predictions.py \
        --input_path data/past_sentence_filtering_preds/text_parser_predicted_2024_02_25.tsv \
        --model_name gpt3.5-ft
"""

import argparse
import glob
import json
import os

import pandas as pd


def aggregate_batch_predictions(batch_input_path, results_dir, output_path):
    """Aggregate OpenAI batch JSONL results with the input TSV rows.

    Reads batch_input_path (columns: custom_id, pmcid, section, matched_query,
    sentence, answer) and all batch_*_results_*.jsonl files in results_dir,
    joins on custom_id, and writes a TSV with columns matching the
    text_parser_predicted format: pmcid, section, matched_query, sentence,
    prob, response.
    """
    # Load input rows keyed by custom_id
    input_df = pd.read_csv(batch_input_path, sep="\t", dtype={"custom_id": str})
    input_rows = {str(row["custom_id"]): row for _, row in input_df.iterrows()}

    # Load all batch result JSONL files
    responses = {}
    result_files = sorted(glob.glob(os.path.join(results_dir, "batch_*_results_*.jsonl")))
    if not result_files:
        raise FileNotFoundError(f"No batch_*_results_*.jsonl files found in {results_dir}")
    for fpath in result_files:
        with open(fpath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                custom_id = str(obj["custom_id"])
                content = obj["response"]["body"]["choices"][0]["message"]["content"]
                responses[custom_id] = content

    # Build output rows sorted by custom_id (numeric order)
    records = []
    for custom_id in sorted(input_rows.keys(), key=lambda x: int(x)):
        row = input_rows[custom_id]
        pmcid_raw = str(row["pmcid"])
        pmcid = int(pmcid_raw.replace("PMC", "")) if pmcid_raw.startswith("PMC") else int(pmcid_raw)
        records.append({
            "pmcid": pmcid,
            "section": row["section"],
            "matched_query": row["matched_query"],
            "sentence": row["sentence"],
            "prob": row["answer"],
            "response": responses.get(custom_id, ""),
        })

    out_df = pd.DataFrame(records, columns=["pmcid", "section", "matched_query", "sentence", "prob", "response"])
    out_df.to_csv(output_path, sep="\t", index=False)
    print(f"Saved {len(out_df)} rows to {output_path}")


def parse_response(response_str):
    """Parse a response cell into a sorted list of [food, part, compound, quantity] triplets."""
    triplets = []
    for line in response_str.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",", 3)]
        # Pad to 4 fields if trailing comma was absent
        while len(parts) < 4:
            parts.append("")
        triplets.append(parts)

    # Sort by formatted string for consistent ordering
    triplets.sort(key=lambda t: "(" + ", ".join(t) + ")")
    return triplets


def tsv_to_json(input_path):
    df = pd.read_csv(input_path, sep="\t")
    output = {}

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
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    print(f"Saved {len(output)} entries to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate batch predictions and/or convert text_parser TSV to JSON")

    # Aggregation args
    parser.add_argument("--batch_input_path", type=str, default=None,
                        help="Path to batch input TSV (triggers aggregation step)")
    parser.add_argument("--batch_results_dir", type=str, default=None,
                        help="Directory containing batch_*_results_*.jsonl files")
    parser.add_argument("--output_tsv", type=str, default=None,
                        help="Output path for aggregated TSV (required when --batch_input_path is set)")

    # TSV-to-JSON args
    parser.add_argument("--input_path", type=str, default=None,
                        help="Path to existing TSV to convert (used when skipping aggregation)")

    args = parser.parse_args()

    if args.batch_input_path:
        if not args.batch_results_dir or not args.output_tsv:
            parser.error("--batch_results_dir and --output_tsv are required with --batch_input_path")
        aggregate_batch_predictions(args.batch_input_path, args.batch_results_dir, args.output_tsv)
        tsv_path = args.output_tsv
    else:
        if not args.input_path:
            parser.error("Either --batch_input_path or --input_path must be provided")
        tsv_path = args.input_path

    tsv_to_json(tsv_path)
