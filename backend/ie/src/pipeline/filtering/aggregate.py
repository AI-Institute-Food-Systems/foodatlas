"""Step 3: Aggregate BioBERT sentence filtering results and deduplicate."""

from __future__ import annotations

import argparse
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


def strip_pmc(series: Any) -> Any:
    """Normalise PMCIDs to bare integers (strip 'PMC' prefix)."""
    return series.astype(str).str.removeprefix("PMC").astype(int)


def aggregate_food_chem_sentences(
    input_dir: str,
    aggregated_path: str,
    ie_input_path: str,
    reference_dir: str,
    threshold: float,
) -> None:
    """Aggregate filtered sentences and remove already-seen PMCIDs."""
    input_path = Path(input_dir)
    tsv_files = sorted(input_path.glob("*.tsv"))

    if not tsv_files:
        log.info("No TSV files found in %s", input_dir)
        return

    log.info("Found %d TSV files", len(tsv_files))

    all_dataframes: list[pd.DataFrame] = []
    for file_path in tsv_files:
        df = pd.read_csv(file_path, sep="\t")
        all_dataframes.append(df)

    all_sentences = pd.concat(all_dataframes, ignore_index=True)
    passed = all_sentences[all_sentences["answer"] >= threshold]

    if passed.empty:
        log.info("No rows passed threshold %f", threshold)
    else:
        agg_path = Path(aggregated_path)
        agg_path.parent.mkdir(parents=True, exist_ok=True)
        passed.to_csv(aggregated_path, sep="\t", index=False)
        log.info(
            "Saved %d rows (threshold=%f) to %s",
            len(passed),
            threshold,
            aggregated_path,
        )

    reference_pmcids: set[int] = set()
    ref_path = Path(reference_dir)
    ref_files = sorted(ref_path.glob("text_parser_predicted_*"))
    if ref_files:
        ref_frames = [pd.read_csv(f, sep="\t") for f in ref_files]
        reference_pmcids = set(
            strip_pmc(pd.concat(ref_frames, ignore_index=True)["pmcid"])
        )
        log.info(
            "Loaded %d reference PMCIDs from %d file(s) in %s",
            len(reference_pmcids),
            len(ref_files),
            reference_dir,
        )
    else:
        log.info("No reference files found in %s", reference_dir)

    unseen = passed[~strip_pmc(passed["pmcid"]).isin(reference_pmcids)]
    ie_path = Path(ie_input_path)
    ie_path.parent.mkdir(parents=True, exist_ok=True)
    unseen.to_csv(ie_input_path, sep="\t", index=False)
    log.info(
        "Saved %d new sentences (%d PMCIDs not shared with reference) to %s",
        len(unseen),
        unseen["pmcid"].nunique(),
        ie_input_path,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate food-chemical sentences filtered by BioBERT probability"
        ),
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        default="outputs/text_parser/sentence_filtering",
    )
    parser.add_argument(
        "--aggregated_path",
        type=str,
        default=(
            "outputs/text_parser/filtered_sentences/filtered_sentence_aggregated.tsv"
        ),
    )
    parser.add_argument(
        "--ie_input_path",
        type=str,
        default=(
            "outputs/text_parser/filtered_sentences/information_extraction_input.tsv"
        ),
    )
    parser.add_argument(
        "--reference_dir",
        type=str,
        default="outputs/past_sentence_filtering_preds",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.99,
    )

    args = parser.parse_args()
    aggregate_food_chem_sentences(
        args.input_dir,
        args.aggregated_path,
        args.ie_input_path,
        args.reference_dir,
        args.threshold,
    )
